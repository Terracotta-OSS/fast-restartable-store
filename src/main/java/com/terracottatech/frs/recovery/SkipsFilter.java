/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionDecodeException;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogRecord;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class SkipsFilter extends AbstractAdaptingFilter<LogRecord, Action> {
  private final Set<Long> skips = new HashSet<Long>();
  private final ActionManager actionManager;

  public SkipsFilter(Filter<Action> nextFilter, ActionManager actionManager) {
    super(nextFilter);
    this.actionManager = actionManager;
  }

  @Override
  public boolean filter(LogRecord element, long lsn) {
    if (skips.contains(element.getLsn())) {
      skips.add(element.getPreviousLsn());
    } else {
      if (delegate(element, lsn)) {
        skips.add(element.getPreviousLsn());
        return true;
      }
    }
    return false;
  }

  @Override
  protected Action convert(LogRecord element) {
    try {
      return actionManager.extract(element);
    } catch (ActionDecodeException e) {
      // Replace this with some better exception handling
      throw new RuntimeException("Error decoding action!");
    }
  }
}
