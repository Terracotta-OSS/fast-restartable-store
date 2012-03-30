/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tim
 */
public class TransactionManagerImpl implements TransactionManager {
  private final AtomicLong currentTransactionId = new AtomicLong();
  private final Set<TransactionHandle> liveTransactions = Collections.newSetFromMap(new ConcurrentHashMap<TransactionHandle, Boolean>());

  private final ActionManager actionManager;
  private final boolean waitOnCommit;

  public TransactionManagerImpl(ActionManager actionManager, boolean waitOnCommit) {
    this.actionManager = actionManager;
    this.waitOnCommit = waitOnCommit;
  }

  @Override
  public TransactionHandle begin() {
    TransactionHandle handle = new TransactionHandleImpl(currentTransactionId.incrementAndGet());
    liveTransactions.add(handle);
    actionManager.asyncHappened(new TransactionBeginAction(handle));
    return handle;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void commit(TransactionHandle handle) throws InterruptedException, TransactionException {
    if (!liveTransactions.remove(handle)) {
      throw new IllegalArgumentException(handle + " does not belong to a live transaction.");
    }
    Future<Void> commitFuture = actionManager.happened(new TransactionCommitAction(handle));
    if (waitOnCommit) {
      try {
        commitFuture.get();
      } catch (ExecutionException e) {
        throw new TransactionException("Commit failed.", e);
      }
    }
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
    if (!liveTransactions.contains(handle)) {
      throw new IllegalArgumentException(handle + " does not belong to a live transaction.");
    }
    Action transactionalAction = new TransactionalAction(handle, action);
    actionManager.asyncHappened(transactionalAction);
  }

  static class TransactionHandleImpl implements TransactionHandle {
    private final long id;

    TransactionHandleImpl(long id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionHandleImpl that = (TransactionHandleImpl) o;

      return id == that.id;
    }

    @Override
    public int hashCode() {
      return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
      return "TransactionHandleImpl{" +
              "id=" + id +
              '}';
    }
  }
}
