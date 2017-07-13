/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import org.junit.Test;

import com.terracottatech.frs.log.LogRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionManagerImplConcurrencyTest extends BaseActionManagerImplTest {

  @Test
  public void testHighlyConcurrentActionsWithPeriodicPauseResume() throws Exception {
    when(logMgr.append(any(LogRecord.class))).thenAnswer(answerOnAppend(true, false, 20));
    when(logMgr.appendAndSync(any(LogRecord.class))).thenAnswer(answerOnAppend(true, false, 20));

    Future<Void> pauseResumeFuture = runPauseResumeTask();
    List<Future<Void>> happenedFutures = submitHappenedTasks(1000, false);
    happenedFutures.addAll(submitHappenedTasks(500, true));

    pauseResumeFuture.get();
    assertThat(pauseResumeFuture.isDone(), is(true));

    for (Future<Void> f : happenedFutures) {
      f.get();
      assertThat(f.isDone(), is(true));
    }
  }

  private Future<Void> runPauseResumeTask() {
    final Random rand = new Random();
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    return executorService.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        for (int i = 0; i < 200; i++) {
          actionMgr.pause();
          long counterAfterPause = getAppendTaskCounter();
          try {
            Thread.sleep(rand.nextInt(30));
          } catch (InterruptedException ignored) {
          }
          long counterBeforeResume = getAppendTaskCounter();
          actionMgr.resume();
          // assert that appends are not happening during pause
          assertThat(counterAfterPause, is(counterBeforeResume));
          try {
            Thread.sleep(rand.nextInt(20));
          } catch (InterruptedException ignored) {
          }
        }
        return null;
      }
    });
  }

  private List<Future<Void>> submitHappenedTasks(int numTasks, final boolean synchronous) {
    ExecutorService executorService = Executors.newFixedThreadPool(20);
    List<Future<Void>> futureTasks = new ArrayList<Future<Void>>();
    for (int i = 0; i < numTasks; i++) {
      futureTasks.add(executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          Action put = mock(Action.class);
          if (synchronous) {
            Future<Void> actionFuture = actionMgr.happened(put);
            Future<Void> actionFuture1 = actionMgr.syncHappened(put);
            actionFuture1.get();
            actionFuture.get();
            assertThat(actionFuture1.isDone(), is(true));
            assertThat(actionFuture.isDone(), is(true));
          } else {
            List<Future<Void>> futureActions = new ArrayList<Future<Void>>();
            for (int i = 0; i < 10; i++) {
              futureActions.add(actionMgr.happened(put));
            }
            for (Future<Void> f : futureActions) {
              f.get();
              assertThat(f.isDone(), is(true));
            }
          }
          return null;
        }
      }));
    }
    return futureTasks;
  }
}