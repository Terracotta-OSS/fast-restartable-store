/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.MasterLogRecordFactory;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseActionManagerImplTest {
  private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
  private final ExecutorService threadPoolExecutor = Executors.newFixedThreadPool(20);
  private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(5);

  private final Random testRandom = new Random();
  private final AtomicLong appendTaskCounter = new AtomicLong(0L);

  ActionManager actionMgr;
  LogManager logMgr;


  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    appendTaskCounter.set(0L);
    ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = mock(ObjectManager.class);
    ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> actionCodec = mock(ActionCodec.class);
    when(actionCodec.encode(any(Action.class))).thenReturn(new ByteBuffer[] {byteBufferWithInt(10)});
    when(actionCodec.decode(any(ByteBuffer[].class))).thenReturn(mock(Action.class));
    logMgr = mock(LogManager.class);
    actionMgr = new ActionManagerImpl(logMgr, objectManager, actionCodec, new MasterLogRecordFactory());
  }

  Answer<Future<Void>> answerOnAppend(final boolean random, final boolean single, final int higherLimit) {
    return new Answer<Future<Void>>() {
      @Override
      public Future<Void> answer(InvocationOnMock invocationOnMock) throws Throwable {
        Callable<Void> appendTask = new AppendTask(random ? testRandom.nextInt(higherLimit) : higherLimit);
        ExecutorService executorService = (single) ? singleThreadExecutor : threadPoolExecutor;
        appendTaskCounter.incrementAndGet();
        return executorService.submit(appendTask);
      }
    };
  }

  ScheduledFuture<Void> scheduleResumeTask(int delayInMillis) {
    return scheduledExecutor.schedule(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        actionMgr.resume();
        return null;
      }
    }, delayInMillis, TimeUnit.MILLISECONDS);
  }

  long getAppendTaskCounter() {
    return appendTaskCounter.get();
  }

  private class AppendTask implements Callable<Void> {
    private final int sleepTimeMillis;

    private AppendTask(int sleepTimeMillis) {
      this.sleepTimeMillis = sleepTimeMillis;
    }

    @Override
    public Void call() throws Exception {
      if (sleepTimeMillis > 0) {
        Thread.sleep(sleepTimeMillis);
      }
      return null;
    }
  }
}