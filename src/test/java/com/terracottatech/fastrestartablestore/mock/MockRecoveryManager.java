/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Filter;
import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.RecoveryManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Iterator;

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

    Filter<Action> replay = new MockReplayFilter(objManager);
    Filter<Action> deleteFilter = new MockDeleteFilter(replay);
    Filter<Action> transactionFilter = new MockTransactionFilter(deleteFilter);
    
    Filter<LogRecord> skipsFilter = new MockSkipsFilter(rcdManager, transactionFilter);
    while (it.hasNext()) {
      LogRecord record = it.next();
      skipsFilter.filter(record, record.getLsn());
    }
  }
}
