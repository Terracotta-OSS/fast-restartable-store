/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author tim
 */
public class CompactorImpl implements Compactor {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);

  private static final long RUN_INTERVAL_SECONDS = 60;
  private static final int START_THRESHOLD = 1000;
  private static final double MINIMUM_LOAD = 0.60;
  private static final double MAXIMUM_LOAD = 0.85;

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager transactionManager;
  private final LogManager logManager;
  private final Semaphore compactionCondition = new Semaphore(0);
  private final AtomicBoolean alive = new AtomicBoolean();

  private CompactorThread compactorThread;

  public CompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, TransactionManager transactionManager, LogManager logManager) {
    this.objectManager = objectManager;
    this.transactionManager = transactionManager;
    this.logManager = logManager;
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

          long currentLsn = logManager.currentLsn();
          long lowestLsn = objectManager.getLowestLsn();
          long entries = currentLsn - lowestLsn;
          long liveEntries = objectManager.size();
          if (((double) liveEntries) / entries <= MINIMUM_LOAD) {
            compact(currentLsn, liveEntries);
          }
        } catch (Exception e) {
          LOGGER.error("Error performing compaction.", e);
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void compact(long currentLsn, long liveSize) throws TransactionException,
          InterruptedException {
    compactionCondition.drainPermits();
    for (long i = 0; i < liveSize; i++) {
      ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> compactionEntry = objectManager.acquireCompactionEntry();
      if (compactionEntry != null) {
        try {
          // The way this termination condition works is by checking the approximate length
          // of the LSN span against the live object count. As entries are compacted over, the
          // window shrinks in length until it equals the live object count.
          double compactedLoadRatio = liveSize / ((double) (currentLsn + i - compactionEntry.getLsn()));
          if (compactedLoadRatio >= MAXIMUM_LOAD) {
            break;
          }

          CompactionAction compactionAction =
                  new CompactionAction(objectManager, compactionEntry);
          transactionManager.happened(compactionAction);
          // We can't update the object manager on Action.record() because the compactor
          // is holding onto the segment lock. Since we want to wait for the action to be
          // sequenced anyways so we don't keep getting the same compaction keys, we may as
          // well just do the object manager update here.
          compactionAction.updateObjectManager();
        } finally {
          objectManager.releaseCompactionEntry(compactionEntry);
        }
      }
    }
    objectManager.updateLowestLsn();
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
