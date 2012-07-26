/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.DeleteFilter;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.transaction.TransactionFilter;
import com.terracottatech.frs.util.NullFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

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

  public RecoveryManagerImpl(LogManager logManager, ActionManager actionManager, Configuration configuration) {
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.compressedSkipSet = configuration.getBoolean(FrsProperty.RECOVERY_COMPRESSED_SKIP_SET);
    this.replayFilter = new ReplayFilter(configuration.getInt(FrsProperty.RECOVERY_MIN_THREAD_COUNT),
                                         configuration.getInt(FrsProperty.RECOVERY_MAX_THREAD_COUNT),
                                         configuration.getInt(FrsProperty.RECOVERY_REPLAY_BATCH_SIZE),
                                         configuration.getDBHome());
    this.configuration = configuration;
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
        progressLoggingFilter.filter(action, logRecord.getLsn(), false);
        ntime = System.nanoTime();
        put += (ntime - ctime);
        replayFilter.checkError();
        lastRecoveredLsn = logRecord.getLsn();
      }
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
//      double currentProgress = progress(lsn);
      if (count-- <= 0 && position > 0) {
        LOGGER.info("Recovery progress " + (10 - position)*10 + "%");
        count = (lsn - lowestLsn)/position--;
      } 
      
      if ( lsn == lowestLsn ) {
        LOGGER.info("Recovery progress 100%");
      }
      return delegate(element, lsn, filtered);
    }

//    private double progress(long current) {
//      return (1.0 - ((double) current) / totalLsns);
//    }
  }

  private static class ReplayFilter implements Filter<Action>, ThreadFactory {
    private final AtomicInteger              threadId        = new AtomicInteger();
    private final AtomicReference<Throwable> firstError      =
            new AtomicReference<Throwable>();

    private final ExecutorService            executorService;
    private final File dbHome;
    private final int replayBatchSize;
    private long replayed = 0;
    private List<ReplayElement> batch;

    ReplayFilter(int minThreadCount, int maxThreadCount, int replayBatchSize, File dbHome) {
      executorService = new ThreadPoolExecutor(minThreadCount,
                                               maxThreadCount, 60,
                                               SECONDS,
                                               new SynchronousQueue<Runnable>(),
                                               this,
                                               new ThreadPoolExecutor.CallerRunsPolicy());
      this.dbHome = dbHome;
      this.replayBatchSize = replayBatchSize;
      batch = new ArrayList<ReplayElement>(replayBatchSize);
    }
    
    public long getReplayCount() {
        return replayed;
    }

    @Override
    public boolean filter(final Action element, final long lsn, boolean filtered) {
      if (filtered) {
        return false;
      } else {
        replayed++;
        batch.add(new ReplayElement(element,lsn));
        if ( batch.size() >= replayBatchSize ) {
            submitJob();
        }

        return true;
      }
    }
    
    private void submitJob() {
        if ( batch.isEmpty() ) return;
        
        final List<ReplayElement> go = batch;
        batch = new ArrayList<ReplayElement>(replayBatchSize);
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    for ( ReplayElement a : go ) {
                        a.replay();
                    }
                } catch (Throwable t) {
                firstError.compareAndSet(null, t);
                LOGGER.error("Error replaying record: " + t.getMessage());
                }
            }
            });
    }

    void checkError() throws RecoveryException {
      Throwable t = firstError.get();
      if (t != null) {
        throw new RecoveryException("Caught an error recovering from log at " + dbHome.getAbsolutePath(), t);
      }
    }

    void finish() throws InterruptedException {
      submitJob();
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, SECONDS);
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("Replay Thread - " + threadId.getAndIncrement());
      t.setDaemon(true);
      return t;
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
      }
    }
}
