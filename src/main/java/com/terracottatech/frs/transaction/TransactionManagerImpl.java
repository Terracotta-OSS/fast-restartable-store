/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
public class TransactionManagerImpl implements TransactionManager {
  private final AtomicLong                               currentTransactionId =
          new AtomicLong();
  private final Map<TransactionHandle, Collection<Lock>> liveTransactions     =
          new ConcurrentHashMap<TransactionHandle, Collection<Lock>>();

  private final ActionManager           actionManager;
  private final TransactionLockProvider transactionLockProvider;
  private final boolean                 waitOnCommit;

  public TransactionManagerImpl(ActionManager actionManager, boolean waitOnCommit) {
    this(actionManager, new TransactionLockProviderImpl(), waitOnCommit);
  }

  // Used for tests in order to pass in a TransactionLockProvider
  TransactionManagerImpl(ActionManager actionManager, TransactionLockProvider transactionLockProvider,
                         boolean waitOnCommit) {
    this.actionManager = actionManager;
    this.transactionLockProvider = transactionLockProvider;
    this.waitOnCommit = waitOnCommit;
  }

  @Override
  public TransactionHandle begin() {
    TransactionHandle handle =
            new TransactionHandleImpl(currentTransactionId.incrementAndGet());
    liveTransactions.put(handle, new ArrayList<Lock>());
    actionManager.asyncHappened(new TransactionBeginAction(handle));
    return handle;
  }

  @Override
  public void commit(TransactionHandle handle) throws InterruptedException,
          TransactionException {
    Collection<Lock> locks = liveTransactions.remove(handle);
    if (locks == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    Future<Void> commitFuture =
            actionManager.happened(new TransactionCommitAction(handle));
    try {
      if (waitOnCommit) {
        try {
          commitFuture.get();
        } catch (ExecutionException e) {
          throw new TransactionException("Commit failed.", e);
        }
      }
    } finally {
      for (Lock lock : locks) {
        lock.unlock();
      }
    }
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
    Collection<Lock> locks = liveTransactions.get(handle);
    if (locks == null) {
      throw new IllegalArgumentException(
              handle + " does not belong to a live transaction.");
    }
    Action transactionalAction = new TransactionalAction(handle, action);
    locks.addAll(transactionalAction.lock(transactionLockProvider));
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
