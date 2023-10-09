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

import org.junit.Test;

import com.terracottatech.frs.log.LogRecord;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ActionManagerImplTest extends BaseActionManagerImplTest {

  @Test
  public void testHappenedWhenNormal() throws Exception {
    Action put = mock(Action.class);
    when(logMgr.append(any(LogRecord.class))).thenAnswer(answerOnAppend(false, true, 0));
    Future<Void> actionFuture = actionMgr.happened(put);
    actionFuture.get();
    assertThat(actionFuture.isDone(), is(true));
  }

  @Test
  public void testSyncHappenedWhenNormal() throws Exception {
    Action put = mock(Action.class);
    when(logMgr.appendAndSync(any(LogRecord.class))).thenAnswer(answerOnAppend(false, true, 0));
    Future<Void> actionFuture = actionMgr.syncHappened(put);
    actionFuture.get();
    assertThat(actionFuture.isDone(), is(true));
  }

  @Test
  public void testHappenedWhenPaused() throws Exception {
    Action put1 = mock(Action.class);
    Action put2 = mock(Action.class);
    when(logMgr.append(any(LogRecord.class))).thenAnswer(answerOnAppend(false, true, 0));
    actionMgr.pause();
    scheduleResumeTask(100);
    Future<Void> actionFuture1 = actionMgr.happened(put1);
    Future<Void> actionFuture2 = actionMgr.happened(put2);
    actionFuture1.get();
    actionFuture2.get();
    assertThat(actionFuture1.isDone(), is(true));
    assertThat(actionFuture2.isDone(), is(true));
  }

  @Test
  public void testSyncHappenedWhenPaused() throws Exception {
    Action put = mock(Action.class);
    when(logMgr.appendAndSync(any(LogRecord.class))).thenAnswer(answerOnAppend(false, true, 0));
    actionMgr.pause();
    scheduleResumeTask(100);
    Future<Void> actionFuture = actionMgr.syncHappened(put);
    actionFuture.get(10, TimeUnit.MILLISECONDS);
    assertThat(actionFuture.isDone(), is(true));
  }

  @Test
  public void testPausedWhenHappenedTakesLonger() throws Exception {
    Action put = mock(Action.class);
    when(logMgr.append(any(LogRecord.class))).thenAnswer(answerOnAppend(false, false, 100));
    when(logMgr.appendAndSync(any(LogRecord.class))).thenAnswer(answerOnAppend(false, false, 0));
    Future<Void> actionFuture =  actionMgr.happened(put);
    Future<Void> actionFuture1 = actionMgr.happened(put);
    actionMgr.pause();

    // assert that all actions before pause is completing properly
    actionFuture.get();
    actionFuture1.get();
    assertThat(actionFuture.isDone(), is(true));
    assertThat(actionFuture1.isDone(), is(true));
    scheduleResumeTask(10);

    actionFuture = actionMgr.syncHappened(put);
    actionFuture.get();
    assertThat(actionFuture.isDone(), is(true));
  }
}