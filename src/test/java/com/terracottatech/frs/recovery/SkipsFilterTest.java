/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.*;
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
    filter = new SkipsFilter(delegate, 5L, true);
  }

  @Test
  public void testFilterBelowLowest() throws Exception {
    Action action4 = createAction(true);
    Action action5 = createAction(4L, true);
    assertThat(filter.filter(action5, 5), is(true));
    // This is weird in that 4 should be skipped, but since it's below the lowestLsn,
    // the skips filter won't track it. To test that, set 4 to be skipped, and try to
    // replay it.
    assertThat(filter.filter(action4, 4), is(true));
  }

  @Test
  public void testSkipping() throws Exception {
    Action action6 = createAction(false);
    Action action7 = createAction(true);
    Action action8 = createAction(Arrays.asList(6L, 7L), false);
    Action action9 = createAction(8, true);
    assertThat(filter.filter(action9, 9), is(true));
    assertThat(filter.filter(action8, 8), is(false));
    assertThat(filter.filter(action7, 7), is(false));
    assertThat(filter.filter(action6, 6), is(false));
  }

  @Test
  public void testDelegateReturnsFalse() throws Exception {
    Action action5 = createAction(-1, true);
    Action action6 = createAction(5, false);
    assertThat(filter.filter(action6, 6), is(false));
    assertThat(filter.filter(action5, 5), is(true));
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
