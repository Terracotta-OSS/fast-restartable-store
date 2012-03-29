/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.mock.MockDeleteFilter;
import com.terracottatech.frs.mock.transaction.MockTransactionFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.action.RecordManager;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;

import java.util.Iterator;

/**
 *
 * @author cdennis
 */
public class MockRecoveryManager implements RecoveryManager {

  private final LogManager logManager;
  private final RecordManager rcdManager;

  public MockRecoveryManager(LogManager logManager, RecordManager rcdManager) {
    this.logManager = logManager;
    this.rcdManager = rcdManager;
  }

  @Override
  public void recover() {
    Iterator<LogRecord> it = logManager.reader();

    Filter<Action> replay = new MockReplayFilter();
    Filter<Action> deleteFilter = new MockDeleteFilter(replay);
    Filter<Action> transactionFilter = new MockTransactionFilter(deleteFilter);
    
    Filter<LogRecord> skipsFilter = new MockSkipsFilter(rcdManager, transactionFilter);
    while (it.hasNext()) {
      LogRecord record = it.next();
      skipsFilter.filter(record, record.getLsn());
    }
  }
}
