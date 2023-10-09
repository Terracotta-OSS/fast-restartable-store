/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import static org.mockito.ArgumentMatchers.any;
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