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

  public void recover() {
    Iterator<LogRecord> it = logManager.reader();
    Set<Long> skips = new HashSet<Long>();
    
    while (it.hasNext()) {
      LogRecord record = it.next();
      if (skips.remove(record.getLsn())) {
        skips.add(record.getPreviousLsn());
      } else {
        Action action = rcdManager.extract(record);
        if (objManager.replay(action, record.getLsn())) {
           skips.add(record.getPreviousLsn());
        }         
      }
    }
  }
}
