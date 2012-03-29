/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.recovery;

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
    return actionManager.extract(element);
  }
  
}
