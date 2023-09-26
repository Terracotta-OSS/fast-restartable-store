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
package com.terracottatech.frs.recovery;

import org.junit.Rule;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.NullLogManager;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class AbstractRecoveryManagerImplTest {
  @Rule
  public JUnitTestFolder testFolder = new JUnitTestFolder();

  protected ActionManager actionManager;

  LogManager newLogManager() {
    LogManager lm = new RecoveryTestLogManager();
    return spy(lm);
  }

  ActionManager newActionManager() {
    return mock(ActionManager.class);
  }

  LogRecord record(long lsn, Action action) throws Exception {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    doReturn(action).when(actionManager).extract(record);
    return record;
  }

  Action action(long previousLsn, boolean shouldReplay) {
    InvalidatingAction action = mock(InvalidatingAction.class);
    doReturn(Collections.singleton(previousLsn)).when(action).getInvalidatedLsns();
    if (!shouldReplay) {
      doThrow(new AssertionError("Should not have been executed.")).when(action).replay(
          anyLong());
    }
    return action;
  }

  Action action(boolean shouldReplay) {
    Action action = mock(Action.class);
    if (!shouldReplay) {
      doThrow(new AssertionError("Should not have been executed.")).when(action).replay(
          anyLong());
    }
    return action;
  }

  private class RecoveryTestLogManager extends NullLogManager {
    private final List<LogRecord> records = new LinkedList<>();
    private long lowestLsn = Long.MAX_VALUE;

    @Override
    public Future<Void> append(LogRecord record) {
      records.add(0, record);
      return mock(Future.class);
    }

    @Override
    public Future<Void> appendAndSync(LogRecord record) {
      return append(record);
    }

    @Override
    public Iterator<LogRecord> startup() {
      return records.iterator();
    }

    @Override
    public void updateLowestLsn(long lsn) {
      lowestLsn = lsn;
    }

    @Override
    public long lowestLsn() {
      return lowestLsn;
    }
  }
}
