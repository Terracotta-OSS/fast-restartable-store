/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.NullAction;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author tim
 */
public class CompactorImpl implements Compactor {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);

  private static final long RUN_INTERVAL_SECONDS = 300;
  private static final long COMPACT_ACTION_THROTTLE = 1000;
  private static final int START_THRESHOLD = 50000;

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager transactionManager;
  private final LogManager logManager;
  private final Semaphore compactionCondition = new Semaphore(0);
  private final AtomicBoolean alive = new AtomicBoolean();
  private final CompactionPolicy policy;

  private CompactorThread compactorThread;

  public CompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, TransactionManager transactionManager, LogManager logManager, CompactionPolicy policy) {
    this.objectManager = objectManager;
    this.transactionManager = transactionManager;
    this.logManager = logManager;
    this.policy = policy;
  }

  @Override
  public void startup() {
    if (alive.compareAndSet(false, true)) {
      compactorThread = new CompactorThread();
      compactorThread.start();
    }
  }

  @Override
  public void shutdown() throws InterruptedException {
    if (alive.compareAndSet(true, false)) {
      compactionCondition.release(START_THRESHOLD); // Force the compactor thread to wake up
      compactorThread.join();
    }
  }

  private class CompactorThread extends Thread {
    CompactorThread() {
      setDaemon(true);
      setName("CompactorThread");
    }

    @Override
    public void run() {
      while (alive.get()) {
        try {
          compactionCondition.tryAcquire(START_THRESHOLD, RUN_INTERVAL_SECONDS, SECONDS);

          if (!alive.get()) {
            return;
          }

          objectManager.updateLowestLsn();

          if (policy.shouldCompact()) {
            compact();
          }

          logManager.updateLowestLsn(objectManager.getLowestLsn());

          // Flush the new lowest LSN with a dummy record
          transactionManager.asyncHappened(new NullAction()).get();
        } catch (Exception e) {
          LOGGER.error("Error performing compaction.", e);
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void compact() throws TransactionException,
          InterruptedException, ExecutionException {
    compactionCondition.drainPermits();
    policy.startedCompacting();
    long ceilingLsn = transactionManager.getLowestOpenTransactionLsn();
    long liveSize = objectManager.size();
    long compactedCount = 0;
    while (compactedCount < liveSize) {
      ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> compactionEntry = objectManager.acquireCompactionEntry(ceilingLsn);
      if (compactionEntry != null) {
        compactedCount++;
        try {
          CompactionAction compactionAction =
                  new CompactionAction(objectManager, compactionEntry);
          Future<Void> written = transactionManager.asyncHappened(compactionAction);
          // We can't update the object manager on Action.record() because the compactor
          // is holding onto the segment lock. Since we want to wait for the action to be
          // sequenced anyways so we don't keep getting the same compaction keys, we may as
          // well just do the object manager update here.
          compactionAction.updateObjectManager();

          // To prevent filling up the write queue with compaction junk, risking crowding
          // out actual actions, we throttle a bit after some set number of compaction
          // actions by just waiting until the latest compaction action is written to disk.
          if (compactedCount % COMPACT_ACTION_THROTTLE == 0) {
            written.get();
          }

          // Check with the policy if we need to stop.
          if (!policy.compacted(compactionEntry)) {
            break;
          }
        } finally {
          objectManager.releaseCompactionEntry(compactionEntry);
        }
      }
    }
    objectManager.updateLowestLsn();
    policy.stoppedCompacting();
  }

  @Override
  public void generatedGarbage() {
    compactionCondition.release();
  }

  @Override
  public void compactNow() {
    compactionCondition.release(START_THRESHOLD);
  }
}
