/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogRecord;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class SkipsFilterTest {
  private ActionManager actionManager;
  private Filter<Action> delegate;
  private Action action;
  private SkipsFilter filter;

  @Before
  public void setUp() throws Exception {
    action = mock(Action.class);
    actionManager = mock(ActionManager.class);
    doReturn(action).when(actionManager).extract(any(LogRecord.class));

    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong());
    filter = new SkipsFilter(delegate, actionManager);
  }

  @Test
  public void testSkipping() throws Exception {
    LogRecord record1 = createRecord(1, -1, true);
    LogRecord record2 = createRecord(2, 1, true);
    assertThat(filter.filter(record2, 2), is(true));
    assertThat(filter.filter(record1, 1), is(false));
  }

  @Test
  public void testDelegateReturnsFalse() throws Exception {
    LogRecord record1 = createRecord(1, -1, true);
    LogRecord record2 = createRecord(2, 1, false);
    assertThat(filter.filter(record2, 2), is(false));
    assertThat(filter.filter(record1, 1), is(true));
  }

  private LogRecord createRecord(long lsn, long previousLsn, boolean replayReturn) throws Exception {
    LogRecord record = mock(LogRecord.class);
    doReturn(lsn).when(record).getLsn();
    doReturn(previousLsn).when(record).getPreviousLsn();
    Action logRecordAction = mock(Action.class);
    doReturn(logRecordAction).when(actionManager).extract(record);
    doReturn(replayReturn).when(delegate).filter(eq(logRecordAction), anyLong());
    return record;
  }
}
