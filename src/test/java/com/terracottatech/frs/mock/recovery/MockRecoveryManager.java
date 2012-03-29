/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.mock.MockDeleteFilter;
import com.terracottatech.frs.mock.transaction.MockTransactionFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.log.LogManager;
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
  private final ActionManager actionManager;

  public MockRecoveryManager(LogManager logManager, ActionManager actionManager) {
    this.logManager = logManager;
    this.actionManager = actionManager;
  }

  @Override
  public void recover() {
    Iterator<LogRecord> it = logManager.reader();

    Filter<Action> replay = new MockReplayFilter();
    Filter<Action> deleteFilter = new MockDeleteFilter(replay);
    Filter<Action> transactionFilter = new MockTransactionFilter(deleteFilter);
    
    Filter<LogRecord> skipsFilter = new MockSkipsFilter(actionManager, transactionFilter);
    while (it.hasNext()) {
      LogRecord record = it.next();
      skipsFilter.filter(record, record.getLsn());
    }
  }
}
