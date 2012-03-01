/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.RecoveryFilter;
import com.terracottatech.fastrestartablestore.RecoveryManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockRecoveryManager implements RecoveryManager {

  private final LogManager logManager;
  private final RecordManager rcdManager;
  private final ObjectManager objManager;
  
  MockRecoveryManager(LogManager logManager, RecordManager rcdManager, ObjectManager objManager) {
    this.logManager = logManager;
    this.rcdManager = rcdManager;
    this.objManager = objManager;
  }

  @Override
  public void recover() {
    Iterator<LogRecord> it = logManager.reader();
    
    SkipsFilter recoveryFilter = new SkipsFilter(rcdManager, objManager.createRecoveryFilter());

    while (it.hasNext()) {
      recoveryFilter.replay(it.next());
    }
  }

  private static class SkipsFilter {

    private final Set<Long> skips = new HashSet<Long>();
    private final RecordManager rcdManager;
    private final RecoveryFilter next;
    
    public SkipsFilter(RecordManager rcdManager, RecoveryFilter next) {
      this.rcdManager = rcdManager;
      this.next = next;
    }

    public void replay(LogRecord record) {
      if (skips.remove(record.getLsn())) {
        skips.add(record.getPreviousLsn());
      } else {
        Action action = rcdManager.extract(record);
        if (next.replay(action, record.getLsn())) {
          skips.add(record.getPreviousLsn());
        }
      }
    }
  }
}
