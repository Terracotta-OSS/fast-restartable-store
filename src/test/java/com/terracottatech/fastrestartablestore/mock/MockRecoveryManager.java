/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.FilterRule;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.RecoveryManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author cdennis
 */
public class MockRecoveryManager implements RecoveryManager {

  private final LogManager logManager;
  private final RecordManager rcdManager;
  private final ObjectManager objManager;
  
  private final Set<Long> skips = new HashSet<Long>();
  
  MockRecoveryManager(LogManager logManager, RecordManager rcdManager, ObjectManager objManager) {
    this.logManager = logManager;
    this.rcdManager = rcdManager;
    this.objManager = objManager;
  }

  @Override
  public void recover() {
    Iterator<LogRecord> it = logManager.reader();
    
    ReplayFilter filter = new FilterImpl();
    while (it.hasNext()) {
      LogRecord record = it.next();
      if (skips.remove(record.getLsn())) {
        skips.add(record.getPreviousLsn());
      } else {
        Action action = rcdManager.extract(record);
        if (action.replay(filter, objManager, record.getLsn())) {
          skips.add(record.getPreviousLsn());
        }
      }
    }
  }

  private static class FilterImpl implements ReplayFilter {

    private final List<FilterRule> rules = new LinkedList<FilterRule>();
    
    @Override
    public boolean disallows(Action action) {
      for (FilterRule rule : rules) {
        if (rule.disallows(action)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean allows(Action action) {
      for (FilterRule rule : rules) {
        if (rule.allows(action)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void addRule(FilterRule rule) {
      rules.add(rule);
    }

    @Override
    public boolean removeRule(FilterRule rule) {
      return rules.remove(rule);
    }

  }

}
