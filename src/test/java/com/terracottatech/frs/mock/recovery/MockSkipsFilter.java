/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.recovery.Filter;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
class MockSkipsFilter extends MockAbstractFilter<Action, Action> {

  private final Set<Long> skips = new HashSet<Long>();

  public MockSkipsFilter(Filter<Action> next) {
    super(next);
  }
  
  @Override
  public boolean filter(Action action, long lsn, boolean filtered) {
    if (skips.remove(lsn)) {
      updateSkips(action);
      return delegate(action, lsn, true);
    } else {
      if (delegate(action, lsn, filtered)) {
        updateSkips(action);
        return true;
      } else {
        return false;
      }
    }
  }

  private void updateSkips(Action action) {
    if (action instanceof InvalidatingAction) {
      skips.addAll(((InvalidatingAction) action).getInvalidatedLsns());
    }
  }

  @Override
  protected Action convert(Action element) {
    return element;
  }
}
