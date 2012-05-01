/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tim
 */
public class TransactionManagerImpl implements TransactionManager, TransactionLSNCallback {
  private final AtomicLong                               currentTransactionId =
          new AtomicLong();
  private final Map<TransactionHandle, Long> liveTransactions     =
          new ConcurrentHashMap<TransactionHandle, Long>();

  private final ActionManager           actionManager;
  private final boolean                 synchronousCommit;

  public TransactionManagerImpl(ActionManager actionManager, boolean synchronousCommit) {
    this.actionManager = actionManager;
    this.synchronousCommit = synchronousCommit;
  }

  @Override
  public TransactionHandle begin() {
    TransactionHandle handle =
            new TransactionHandleImpl(currentTransactionId.incrementAndGet());
    liveTransactions.put(handle, Long.MAX_VALUE);
    actionManager.asyncHappened(new TransactionBeginAction(handle, this));
    return handle;
  }

  @Override
  public void commit(TransactionHandle handle) throws InterruptedException,
          TransactionException {
    Long lsn = liveTransactions.remove(handle);
    if (lsn == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    happened(new TransactionCommitAction(handle));
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
    Long lsn = liveTransactions.get(handle);
    if (lsn == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    Action transactionalAction = new TransactionalAction(handle, action);
    actionManager.asyncHappened(transactionalAction);
  }

  @Override
  public void happened(Action action) throws TransactionException, InterruptedException {
    if (synchronousCommit) {
      try {
        actionManager.happened(action).get();
      } catch (ExecutionException e) {
        throw new TransactionException("Commit failed.", e);
      }
    } else {
      actionManager.asyncHappened(action);
    }
  }

  @Override
  public long getLowestOpenTransactionLsn() {
    Long lowest = Long.MAX_VALUE;
    for (long l : liveTransactions.values()) {
      lowest = Math.min(l, lowest);
    }
    return lowest;
  }

  @Override
  public void setLsn(TransactionHandle handle, long lsn) {
    Long old = liveTransactions.put(handle, lsn);
    assert old != null && old == Long.MAX_VALUE;
  }
}
