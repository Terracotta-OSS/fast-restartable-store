/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.RecoveryManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockRecoveryManager implements RecoveryManager {

  private final LogManager logManager;
  private final RecordManager rcdManager;
  private final ObjectManager<String, String> objManager;
  
  MockRecoveryManager(LogManager logManager, RecordManager rcdManager, ObjectManager<String, String> objManager) {
    this.logManager = logManager;
    this.rcdManager = rcdManager;
    this.objManager = objManager;
  }

  public void recover() {
    Iterator<LogRecord> it = logManager.reader();
    Set<Long> committedAndOpenIds = new HashSet<Long>();
    Set<Long> skips = new HashSet<Long>();
    
    while (it.hasNext()) {
      LogRecord record = it.next();
      if (skips.remove(record.getLsn())) {
        skips.add(record.getPreviousLsn());
      } else {
        if (rcdManager.extract(record).replay(objManager, committedAndOpenIds, record.getLsn())) {
          skips.add(record.getPreviousLsn());
        }
      }
    }
  }
}
