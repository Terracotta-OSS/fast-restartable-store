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
import com.terracottatech.frs.recovery.RecoveryListener;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.util.NullFuture;

import java.util.Iterator;
import java.util.concurrent.Future;

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
  public Future<Void> recover(RecoveryListener ... listeners) {
    Iterator<LogRecord> it = logManager.reader();

    Filter<Action> replay = new MockReplayFilter();
    Filter<Action> deleteFilter = new MockDeleteFilter(replay);
    Filter<Action> transactionFilter = new MockTransactionFilter(deleteFilter);
    
    Filter<Action> skipsFilter = new MockSkipsFilter(transactionFilter);
    while (it.hasNext()) {
      LogRecord record = it.next();
      Action action = actionManager.extract(record);
      skipsFilter.filter(action, record.getLsn());
    }

    return new NullFuture();
  }
}
