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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tim
 */
public class TransactionManagerImpl implements TransactionManager {
  private final AtomicLong                               currentTransactionId =
          new AtomicLong();
  private final Map<TransactionHandle, TransactionAccount> liveTransactions     =
          new ConcurrentHashMap<TransactionHandle, TransactionAccount>();

  private final ActionManager           actionManager;

  public TransactionManagerImpl(ActionManager actionManager) {
    this.actionManager = actionManager;
  }

  @Override
  public TransactionHandle begin() {
    TransactionHandle handle =
            new TransactionHandleImpl(currentTransactionId.incrementAndGet());
    TransactionAccount account = new TransactionAccount();
    liveTransactions.put(handle, account);
    return handle;
  }

  @Override
  public void commit(TransactionHandle handle, boolean synchronous) throws InterruptedException,
          TransactionException {
    TransactionAccount account = liveTransactions.remove(handle);
    if (account == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    TransactionCommitAction action = new TransactionCommitAction(handle, account.begin());
    if (synchronous) {
      happened(action);
    } else {
      asyncHappened(action);
    }
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
    TransactionAccount account = liveTransactions.get(handle);
    if (account == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    Action transactionalAction = new TransactionalAction(handle, account.begin(), false, action, account);
    actionManager.happened(transactionalAction);
  }

  @Override
  public void happened(Action action) throws TransactionException, InterruptedException {
    try {
      actionManager.syncHappened(action).get();
    } catch (ExecutionException e) {
      throw new TransactionException("Commit failed.", e);
    }
  }

  @Override
  public Future<Void> asyncHappened(Action action) {
    return actionManager.happened(action);
  }

  @Override
  public long getLowestOpenTransactionLsn() {
    Long lowest = Long.MAX_VALUE;
    for (TransactionAccount account : liveTransactions.values()) {
      lowest = Math.min(account.lsn, lowest);
    }
    return lowest;
  }

  private static class TransactionAccount implements TransactionLSNCallback {
    private long lsn = Long.MAX_VALUE;
    private boolean beginWritten = false;

    synchronized boolean begin() {
      if (beginWritten) {
        return false;
      } else {
        beginWritten = true;
        return true;
      }
    }

    public synchronized void setLsn(long lsn) {
      if (this.lsn == Long.MAX_VALUE) {
        this.lsn = lsn;
      } else {
        // This shouldn't happen as we're getting LSNs in increasing order
        assert lsn > this.lsn;
      }
    }
  }
}
