/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.DeleteFilter;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.transaction.TransactionFilter;
import com.terracottatech.frs.util.NullFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author tim
 */
public class RecoveryManagerImpl implements RecoveryManager {
  private static final String COMPRESSED_SKIP_SET_KEY = "recovery.compressedSkipSet";
  private static final String MAX_REPLAY_QUEUE_LENGTH_KEY = "recovery.maxQueueLength";
  private static final String MIN_REPLAY_THREAD_COUNT_KEY = "recovery.minThreadCount";
  private static final String MAX_REPLAY_THREAD_COUNT_KEY = "recovery.maxThreadCount";

  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryManager.class);

  private final LogManager logManager;
  private final ActionManager actionManager;
  private final boolean compressedSkipSet;
  private final ReplayFilter replayFilter;

  public RecoveryManagerImpl(LogManager logManager, ActionManager actionManager, Configuration configuration) {
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.compressedSkipSet = configuration.getBoolean(COMPRESSED_SKIP_SET_KEY);
    this.replayFilter = new ReplayFilter(configuration.getInt(MIN_REPLAY_THREAD_COUNT_KEY),
                                         configuration.getInt(MAX_REPLAY_THREAD_COUNT_KEY),
                                         configuration.getInt(MAX_REPLAY_QUEUE_LENGTH_KEY));
  }

  @Override
  public Future<Void> recover(RecoveryListener ... listeners) throws RecoveryException,
          InterruptedException {
    logManager.startup();

    Iterator<LogRecord> i = logManager.reader();

    Filter<Action> deleteFilter = new DeleteFilter(replayFilter);
    Filter<Action> transactionFilter = new TransactionFilter(deleteFilter);
    Filter<Action> skipsFilter = new SkipsFilter(transactionFilter, logManager.lowestLsn(),
                                                 compressedSkipSet);

    // For now we're not spinning off another thread for recovery.
    try {
      while (i.hasNext()) {
        LogRecord logRecord = i.next();
        Action action = actionManager.extract(logRecord);
        skipsFilter.filter(action, logRecord.getLsn(), false);
        replayFilter.checkError();
      }
    } finally {
      replayFilter.finish();
      replayFilter.checkError();
    }

    for (RecoveryListener listener : listeners) {
      listener.recovered();
    }

    return new NullFuture();
  }

  private static class ReplayFilter implements Filter<Action>, ThreadFactory {
    private final AtomicInteger              threadId        = new AtomicInteger();
    private final AtomicReference<Throwable> firstError      =
            new AtomicReference<Throwable>();

    private final ExecutorService            executorService;

    ReplayFilter(int minThreadCount, int maxThreadCount, int maxQueueLength) {
      executorService = new ThreadPoolExecutor(minThreadCount,
                                               maxThreadCount, 60,
                                               SECONDS,
                                               new ArrayBlockingQueue<Runnable>(
                                                       maxQueueLength),
                                               this,
                                               new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Override
    public boolean filter(final Action element, final long lsn, boolean filtered) {
      if (filtered) {
        return false;
      } else {
        executorService.submit(new Runnable() {
          @Override
          public void run() {
            try {
              element.replay(lsn);
            } catch (Throwable t) {
              firstError.compareAndSet(null, t);
              LOGGER.error("Error replaying record: " + t.getMessage());
            }
          }
        });
        return true;
      }
    }

    void checkError() throws RecoveryException {
      Throwable t = firstError.get();
      if (t != null) {
        throw new RecoveryException("Caught an error during recovery!", t);
      }
    }

    void finish() throws InterruptedException {
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
}
