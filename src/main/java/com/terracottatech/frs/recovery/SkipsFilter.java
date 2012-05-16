/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.util.CompressedLongSet;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class SkipsFilter extends AbstractFilter<Action> {
  private final long lowestLsn;
  private final Set<Long> skips;

  public SkipsFilter(Filter<Action> nextFilter, long lowestLsn, boolean compressed) {
    super(nextFilter);
    this.lowestLsn  = lowestLsn;
    if (compressed) {
      skips = new CompressedLongSet();
    } else {
      skips = new HashSet<Long>();
    }
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (skips.remove(lsn)) {
      updateSkips(element);
      return delegate(element, lsn, true);
    } else {
      if (delegate(element, lsn, filtered)) {
        updateSkips(element);
        return true;
      } else {
        return false;
      }
    }
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
