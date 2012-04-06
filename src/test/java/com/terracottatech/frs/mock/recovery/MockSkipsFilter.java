/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.action.ActionDecodeException;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
class MockSkipsFilter extends MockAbstractFilter<LogRecord, Action> {

  private final Set<Long> skips = new HashSet<Long>();
  private final ActionManager actionManager;

  public MockSkipsFilter(ActionManager actionManager, Filter<Action> next) {
    super(next);
    this.actionManager = actionManager;
  }
  
  @Override
  public boolean filter(LogRecord record, long lsn) {
    if (skips.remove(lsn)) {
      skips.add(record.getPreviousLsn());
      return false;
    } else {
      if (delegate(record, lsn)) {
        skips.add(record.getPreviousLsn());
        return true;
      } else {
        return false;
      }
    }
  }

  @Override
  protected Action convert(LogRecord element) {
    try {
      return actionManager.extract(element);
    } catch (ActionDecodeException e) {
      throw new RuntimeException();
    }
  }
}
