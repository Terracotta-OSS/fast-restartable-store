/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.NullAction;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_POLICY;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_RETRY_INTERVAL;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_RUN_INTERVAL;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_START_THRESHOLD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_THROTTLE_AMOUNT;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author tim
 */
public class CompactorImpl implements Compactor {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager transactionManager;
  private final ActionManager actionManager;
  private final LogManager logManager;
  private final Semaphore compactionCondition = new Semaphore(0);
  private volatile boolean alive = false;
  private final CompactionPolicy policy;
  private final long runIntervalSeconds;
  private final long retryIntervalSeconds;
  private final long compactActionThrottle;
  private final int startThreshold;

  private CompactorThread compactorThread;
  private volatile boolean signalPause;
  private boolean paused;


  CompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                TransactionManager transactionManager, ActionManager actionManager, LogManager logManager,
                CompactionPolicy policy, long runIntervalSeconds, long retryIntervalSeconds,
                long compactActionThrottle, int startThreshold) {
    this.objectManager = objectManager;
    this.transactionManager = transactionManager;
    this.actionManager = actionManager;
    this.logManager = logManager;
    this.policy = policy;
    this.runIntervalSeconds = runIntervalSeconds;
    this.retryIntervalSeconds = retryIntervalSeconds;
    this.compactActionThrottle = compactActionThrottle;
    this.startThreshold = startThreshold;
  }

  public CompactorImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                       TransactionManager transactionManager, LogManager logManager,
                       IOManager ioManager, Configuration configuration, ActionManager actionManager) throws RestartStoreException {
    this(objectManager, transactionManager, actionManager, logManager,
         getPolicy(configuration, objectManager, logManager, ioManager),
         configuration.getLong(COMPACTOR_RUN_INTERVAL),
         configuration.getLong(COMPACTOR_RETRY_INTERVAL),
         configuration.getLong(COMPACTOR_THROTTLE_AMOUNT),
         configuration.getInt(COMPACTOR_START_THRESHOLD));
  }

  private static CompactionPolicy getPolicy(Configuration configuration,
                                            ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                            LogManager logManager, IOManager ioManager) throws RestartStoreException{
    String policy = configuration.getString(COMPACTOR_POLICY);
    if ("LSNGapCompactionPolicy".equals(policy)) {
      return new LSNGapCompactionPolicy(objectManager, logManager, configuration);
    } else if ("SizeBasedCompactionPolicy".equals(policy)) {
      return new SizeBasedCompactionPolicy(ioManager, objectManager, configuration);
    } else if ("NoCompactionPolicy".equals(policy)) {
      LOGGER.warn("Compactor policy is set to 'NoCompactionPolicy'. No compaction will be done.");
      return new NoCompactionPolicy();
    }
    throw new RestartStoreException("Unknown compaction policy " + policy);
  }

  @Override
  public void startup() {
    if (!alive) {
      alive = true;
      LOGGER.info("using " + policy.getClass().getName() + " compaction policy");
      compactorThread = new CompactorThread();
      compactorThread.start();
    }
  }

  @Override
  public void shutdown() throws InterruptedException {
    if (alive) {
      alive = false;
      compactorThread.interrupt();
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
      while (alive) {
        try {
          compactionCondition.tryAcquire(startThreshold, runIntervalSeconds, SECONDS);

          if (checkForPause()) {
            continue;
          }

          // Flush in a dummy record to make sure everything for the updated lowest lsn
          // is on disk prior to cleaning up to the new lowest lsn.
          // If ObjectManager is empty, use the barrier lsn to invalidate the log
          
          NullAction barrier = new NullAction();
          actionManager.happened(barrier).get();
          
          long lowLsn = objectManager.getLowestLsn();
          
          if ( lowLsn == Constants.ISEMPTY_LSN ) {
              lowLsn = barrier.getLsn();
          }

          if (policy.startCompacting() && alive) {
            try {
              compact();
            } finally {
              policy.stoppedCompacting();
            }
          }

          logManager.updateLowestLsn(lowLsn);

          // Flush the new lowest LSN with a dummy record
          actionManager.syncHappened(new NullAction()).get();
        } catch (InterruptedException e) {
          LOGGER.info("Compactor is interrupted. Shutting down.");
          return;
        } catch (Throwable t) {
          LOGGER.error("Error performing compaction. Temporarily disabling compaction for " + retryIntervalSeconds + " seconds.", t);
          try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(retryIntervalSeconds));
          } catch (InterruptedException e) {
            LOGGER.info("Compactor is interrupted. Shutting down.");
            return;
          }
        }
      }
    }
  }

  private void compact() throws ExecutionException, InterruptedException {
    compactionCondition.drainPermits();
    long ceilingLsn = transactionManager.getLowestOpenTransactionLsn();
    long liveSize = objectManager.size();
    long compactedCount = 0;
    long startTime = System.currentTimeMillis();
    while (compactedCount < liveSize && !signalPause) {
      ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> compactionEntry = objectManager.acquireCompactionEntry(ceilingLsn);
      if (compactionEntry == null) {
        return;
      }
      compactedCount++;
      Future<Void> written;
      try {
        CompactionAction compactionAction =
                new CompactionAction(objectManager, compactionEntry);
        written = actionManager.happened(compactionAction);
        // We can't update the object manager on Action.record() because the compactor
        // is holding onto the segment lock. Since we want to wait for the action to be
        // sequenced anyways so we don't keep getting the same compaction keys, we may as
        // well just do the object manager update here.
        compactionAction.updateObjectManager();
      } finally {
        objectManager.releaseCompactionEntry(compactionEntry);
      }

      // Check with the policy if we need to stop.
      if (!policy.compacted(compactionEntry)) {
        break;
      }

      // To prevent filling up the write queue with compaction junk, risking crowding
      // out actual actions, we throttle a bit after some set number of compaction
      // actions by just waiting until the latest compaction action is written to disk.
      if (compactedCount % compactActionThrottle == 0) {
        // While we're waiting, might as well update the lowest lsn so compaction provides continuous benefit.
        logManager.updateLowestLsn(objectManager.getLowestLsn());
        written.get();
      }
    }
    LOGGER.debug("compacted " + compactedCount + " entries in " + TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-startTime) + " secs.");
  }

  @Override
  public void generatedGarbage(long lsn) {
    compactionCondition.release();
  }

  @Override
  public void compactNow() {
    compactionCondition.release(startThreshold);
  }

  private synchronized boolean checkForPause() throws InterruptedException {
    boolean wasPaused = false;
    if (signalPause) {
      signalPause = false;
      paused = true;
      notifyAll();
      while (paused) {
        wasPaused = true;
        wait();
      }
    }
    return wasPaused;
  }

  @Override
  public synchronized void pause() {
    if (paused) {
      return;
    }
    signalPause = true;
    compactNow();
    boolean interrupted = false;
    while (!paused) {
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public synchronized void unpause() {
    if (!paused) {
      return;
    }
    paused = false;
    notifyAll();
  }
}
