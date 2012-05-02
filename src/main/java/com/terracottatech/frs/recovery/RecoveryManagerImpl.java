/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.DeleteFilter;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.transaction.TransactionFilter;

import java.util.Iterator;

/**
 * @author tim
 */
public class RecoveryManagerImpl implements RecoveryManager {
  private final LogManager logManager;
  private final ActionManager actionManager;
  private final ReplayFilter replayFilter = new ReplayFilter();

  public RecoveryManagerImpl(LogManager logManager, ActionManager actionManager) {
    this.logManager = logManager;
    this.actionManager = actionManager;
  }

  @Override
  public void recover() {
    Iterator<LogRecord> i = logManager.reader();

    Filter<Action> deleteFilter = new DeleteFilter(replayFilter);
    Filter<Action> transactionFilter = new TransactionFilter(deleteFilter);
    Filter<Action> skipsFilter = new SkipsFilter(transactionFilter);

    while (i.hasNext()) {
      LogRecord logRecord = i.next();
      Action action = actionManager.extract(logRecord);
      skipsFilter.filter(action, logRecord.getLsn());
    }
  }

  private static class ReplayFilter implements Filter<Action> {

    @Override
    public boolean filter(Action element, long lsn) {
      element.replay(lsn);
      return true;
    }
  }
}
