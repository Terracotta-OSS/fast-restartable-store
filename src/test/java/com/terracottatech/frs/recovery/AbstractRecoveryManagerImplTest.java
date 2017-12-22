/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
