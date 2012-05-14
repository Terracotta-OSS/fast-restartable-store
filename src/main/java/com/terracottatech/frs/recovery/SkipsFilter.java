/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class SkipsFilter extends AbstractFilter<Action> {
  private final long lowestLsn;
  private final Set<Long> skips = new HashSet<Long>();

  public SkipsFilter(Filter<Action> nextFilter, long lowestLsn) {
    super(nextFilter);
    this.lowestLsn = lowestLsn;
  }

  @Override
  public boolean filter(Action element, long lsn) {
    if (skips.remove(lsn)) {
      updateSkips(element);
    } else {
      if (delegate(element, lsn)) {
        updateSkips(element);
        return true;
      }
    }
    return false;
  }

  private void updateSkips(Action action) {
    if (action instanceof InvalidatingAction) {
      for (long invalid : ((InvalidatingAction) action).getInvalidatedLsns()) {
        if (invalid >= lowestLsn) {
          skips.add(invalid);
        }
      }
    }
  }
}
