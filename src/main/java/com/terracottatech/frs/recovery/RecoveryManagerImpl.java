/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.DeleteFilter;
import com.terracottatech.frs.Disposable;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.transaction.TransactionFilter;
import com.terracottatech.frs.util.NullFuture;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author tim
 */
public class RecoveryManagerImpl implements RecoveryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryManager.class);

  private final LogManager logManager;
  private final ActionManager actionManager;
  private final boolean compressedSkipSet;
  private final ReplayFilter replayFilter;
  private final Configuration configuration;

  RecoveryManagerImpl(LogManager logManager, ActionManager actionManager, Configuration configuration,
                             Runtime runtime) {
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.compressedSkipSet = configuration.getBoolean(FrsProperty.RECOVERY_COMPRESSED_SKIP_SET);
    this.replayFilter = new ReplayFilter(configuration.getInt(FrsProperty.RECOVERY_REPLAY_PER_BATCH_SIZE),
        configuration.getInt(FrsProperty.RECOVERY_REPLAY_TOTAL_BATCH_SIZE_MAX),
        configuration.getDBHome(), runtime.availableProcessors());
    this.configuration = configuration;
  }

  public RecoveryManagerImpl(LogManager logManager, ActionManager actionManager, Configuration configuration) {
    this(logManager, actionManager, configuration, Runtime.getRuntime());
  }

  @Override
  public Future<Void> recover(RecoveryListener ... listeners) throws RecoveryException,
          InterruptedException {
    Iterator<LogRecord> i = logManager.startup();
    long filter = 0;
    long put = 0;
    long ntime = System.nanoTime();

    Filter<Action> deleteFilter = new DeleteFilter(replayFilter);
    Filter<Action> transactionFilter = new TransactionFilter(deleteFilter);
    Filter<Action> skipsFilter = new SkipsFilter(transactionFilter, logManager.lowestLsn(),
                                                 compressedSkipSet);
    Filter<Action> progressLoggingFilter =
            new ProgressLoggingFilter(replayFilter.dbHome, skipsFilter, logManager.lowestLsn());

    // For now we're not spinning off another thread for recovery.
    long lastRecoveredLsn = Long.MAX_VALUE;
    try {
      while (i.hasNext()) {
        LogRecord logRecord = i.next();
        Action action = actionManager.extract(logRecord);
        long ctime = System.nanoTime();
        filter += (ctime - ntime);
        boolean replayed = progressLoggingFilter.filter(action, logRecord.getLsn(), false);
        ntime = System.nanoTime();
        put += (ntime - ctime);
        replayFilter.checkError();
        lastRecoveredLsn = logRecord.getLsn();
        if ( action instanceof Disposable ) {
          if ( !replayed ) {
            ((Disposable)action).dispose();
          } // else taken care of in the filter
        } else {
          logRecord.close();
        }
      }
    } catch ( IOException ioe ) {
      throw new RecoveryException("failed to restart", ioe);
    } finally {
      replayFilter.finish();
      replayFilter.checkError();
    }

    if (lastRecoveredLsn != Long.MAX_VALUE && lastRecoveredLsn > logManager.lowestLsn()) {
      throw new RecoveryException("Recovery is incomplete for log " + configuration.getDBHome() + ". Files may be missing.");
    }

    for (RecoveryListener listener : listeners) {
      listener.recovered();
    }

    LOGGER.debug("count " + replayFilter.getReplayCount() + " put " + put + " filter " + filter);
    LOGGER.debug(skipsFilter.toString());
    return new NullFuture();
  }

  private static class ProgressLoggingFilter extends AbstractFilter<Action> {
    private final long lowestLsn;
    private int position = 10;
    private long count = 0;

    ProgressLoggingFilter(File home, Filter<Action> delegate, long lowestLsn) {
      super(delegate);
      LOGGER.info("Starting recovery for " + home.getAbsolutePath());
      this.lowestLsn = lowestLsn;
    }

    @Override
    public boolean filter(Action element, long lsn, boolean filtered) {
      if (count-- <= 0 && position > 0) {
        LOGGER.info("Recovery progress " + (10 - position)*10 + "%");
        count = (lsn - lowestLsn)/position--;
      }

      if ( lsn == lowestLsn ) {
        LOGGER.info("Recovery progress 100%");
      }
      return delegate(element, lsn, filtered);
    }
  }

  private static class ReplayFilter implements Filter<Action> {
    private final AtomicInteger              threadId        = new AtomicInteger();
    private final AtomicReference<Throwable> firstError      = new AtomicReference<>();
    private final ForkJoinPool replayPool;

    private final File dbHome;
    private final int replayPerBatchSize;
    private final int replayTotalBatchSize;
    private long replayed = 0;
    private long submitted = 0;
    private ReplayElement[][] batches;
    private int[] currentIndices;
    private ForkJoinTask<Void> replayBatchTask;

    ReplayFilter(int replayPerBatchSize, int replayTotalBatchSize, File dbHome, int maxThreadCount) {
      this.dbHome = dbHome;
      this.replayPerBatchSize = replayPerBatchSize;
      this.replayTotalBatchSize = replayTotalBatchSize;
      int numBatches = MaxProcessorsToPrime.getNextPrime(maxThreadCount);
      this.batches = new ReplayElement[numBatches][replayPerBatchSize];
      this.currentIndices = new int[numBatches];
      this.replayBatchTask = null;
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      this.replayPool = new ForkJoinPool(maxThreadCount, pool -> {
        ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        thread.setName("Replay Thread - " + threadId.getAndIncrement());
        thread.setContextClassLoader(classLoader);
        return thread;
      }, null, false);
    }

    public long getReplayCount() {
        return replayed;
    }

    @Override
    public boolean filter(final Action element, final long lsn, boolean filtered) {
      if (filtered) {
        return false;
      } else {
        int idx1 = (element.replayConcurrency() & Integer.MAX_VALUE) % batches.length;
        int idx2 = this.currentIndices[idx1];
        int nextIdx2 = idx2 + 1;
        this.currentIndices[idx1] = nextIdx2;
        submitted++;
        batches[idx1][idx2] = new ReplayElement(element,lsn);
        if (submitted - replayed  >= replayTotalBatchSize || nextIdx2 >= replayPerBatchSize - 1) {
          submitJob(false);
        }
        return true;
      }
    }

    private void submitJob(boolean last) {
      final ReplayElement[][] go = batches;
      if (!last) {
        batches = new ReplayElement[batches.length][replayPerBatchSize];
        currentIndices = new int[batches.length];
      } else {
        batches = null;
        currentIndices = null;
      }
      if (replayBatchTask != null) {
        waitForReplayBatchTask();
      }
      if (replayed != submitted) {
        replayed = submitted;
        replayBatchTask = replayBatch(go);
        if (last) {
          waitForReplayBatchTask();
        }
      }
    }

    private void waitForReplayBatchTask() {
      boolean interrupted = false;
      try {
        // let the current batch complete before we allow interrupts
        while (replayBatchTask != null) {
          try {
            // only one pending batch at any given time
            replayBatchTask.get();
          } catch (ExecutionException e) {
            firstError.compareAndSet(null, e);
            LOGGER.error("Error replaying record: " + e.getMessage());
          } catch (InterruptedException e) {
            interrupted |= Thread.interrupted();
          } finally {
            if (replayBatchTask.isDone()) {
              replayBatchTask = null;
            }
          }
        }
      } finally {
        if (interrupted) {
          // restore interrupt status
          Thread.currentThread().interrupt();
        }
      }
    }

    private ForkJoinTask<Void> replayBatch(ReplayElement[][] go) {
      return replayPool.submit(() -> {
        Arrays.stream(go).filter((rs) -> rs[0] != null).parallel().forEach((rs) -> {
          try {
            for (ReplayElement r : rs) {
              if (r == null) {
                break;
              }
              r.replay();
            }
          } catch (Throwable t) {
            firstError.compareAndSet(null, t);
            LOGGER.error("Error replaying record: " + t.getMessage());
          }
        });
        return null;
      });
    }

    void checkError() throws RecoveryException {
      Throwable t = firstError.get();
      if (t != null) {
        throw new RecoveryException("Caught an error recovering from log at " + dbHome.getAbsolutePath(), t);
      }
    }

    void finish() throws InterruptedException {
      submitJob(true);
      replayPool.shutdown();
      boolean done;
      do {
        done = replayPool.awaitTermination(2, MINUTES);
        if (!done) {
          LOGGER.warn("Unable to ensure recovery completion.");
          LOGGER.warn("Cannot proceed further. Checking Again for recovery completion...");
        }
      } while (!done);
    }
  }

  /**
   * Have a better spread to get better concurrency. Pick up a prime that is close to twice the max processors
   */
  private static class MaxProcessorsToPrime {
    private static final int[] primesTo100 = {23, 31, 41, 53, 61, 71, 83, 97, 137, 149, 157, 167, 179, 181, 191, 211, 223, 241, 269, 293};
    private static final int[] primesTo200 = {317, 337, 359, 379, 397, 409, 439, 479, 509, 557};
    private static final int[] primesTo300 = {599, 691, 797, 887, 997};
    private static final int[] primesTo500 = {1091, 1117, 1217, 1319};
    private static final int primeTo1000 = 2399;

    private static int getNextPrime(int numProcessors) {
      if (numProcessors <= 2) {
        return 3;
      } else if (numProcessors < 100) {
        return primesTo100[numProcessors/5];
      } else if (numProcessors < 200) {
        return primesTo200[(numProcessors - 100)/10];
      } else if (numProcessors < 300) {
        return primesTo300[(numProcessors - 200)/20];
      } else if (numProcessors < 500) {
        return primesTo500[(numProcessors - 300)/50];
      } else {
        return primeTo1000;
      }
    }
  }

  private static class ReplayElement {
    private final Action action;
    private final long lsn;

    private ReplayElement(Action action, long lsn) {
      this.action = action;
      this.lsn = lsn;
    }

    void replay() {
      action.replay(lsn);
      if ( action instanceof Disposable ) {
        ((Disposable)action).dispose();
      }
    }
  }
}
