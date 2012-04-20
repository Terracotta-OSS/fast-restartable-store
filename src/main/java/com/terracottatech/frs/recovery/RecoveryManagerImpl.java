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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

    // TODO: Should we batch the evictions?
    if (!replayFilter.invalidatedLsns.isEmpty()) {
      actionManager.asyncHappened(new RecoveryEvictionAction(replayFilter.invalidatedLsns));
    }
  }

  private static class ReplayFilter implements Filter<Action> {
    private final Set<Long> invalidatedLsns = new HashSet<Long>();

    @Override
    public boolean filter(Action element, long lsn) {
      invalidatedLsns.addAll(element.replay(lsn));
      return true;
    }
  }
}
