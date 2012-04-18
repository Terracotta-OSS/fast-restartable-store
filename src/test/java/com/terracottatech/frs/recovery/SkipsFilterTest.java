/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.log.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

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
  private Filter<Action> delegate;
  private SkipsFilter filter;

  @Before
  public void setUp() throws Exception {
    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong());
    filter = new SkipsFilter(delegate);
  }

  @Test
  public void testSkipping() throws Exception {
    Action action1 = createAction(false);
    Action action2 = createAction(true);
    Action action3 = createAction(Arrays.asList(1L, 2L), false);
    Action action4 = createAction(3, true);
    assertThat(filter.filter(action4, 4), is(true));
    assertThat(filter.filter(action3, 3), is(false));
    assertThat(filter.filter(action2, 2), is(false));
    assertThat(filter.filter(action1, 1), is(false));
  }

  @Test
  public void testDelegateReturnsFalse() throws Exception {
    Action action1 = createAction(-1, true);
    Action action2 = createAction(1, false);
    assertThat(filter.filter(action2, 2), is(false));
    assertThat(filter.filter(action1, 1), is(true));
  }

  private Action createAction(boolean replayReturn) {
    Action action = mock(Action.class);
    doReturn(replayReturn).when(delegate).filter(eq(action), anyLong());
    return action;
  }

  private Action createAction(long previousLsn, boolean replayReturn) {
    return createAction(Collections.singleton(previousLsn), replayReturn);
  }

  private Action createAction(final Collection<Long> previousLsns, boolean replayReturn) {
    InvalidatingAction action = mock(InvalidatingAction.class);
    doReturn(replayReturn).when(delegate).filter(eq(action), anyLong());
    doReturn(new HashSet<Long>(previousLsns)).when(action).getInvalidatedLsns();
    return action;
  }
}
