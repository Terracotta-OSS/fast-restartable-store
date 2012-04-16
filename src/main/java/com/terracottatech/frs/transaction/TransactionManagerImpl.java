/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
  private final boolean                 synchronousCommit;

  public TransactionManagerImpl(ActionManager actionManager, boolean synchronousCommit) {
    this(actionManager, new TransactionLockProviderImpl(), synchronousCommit);
  }

  // Used for tests in order to pass in a TransactionLockProvider
  TransactionManagerImpl(ActionManager actionManager, TransactionLockProvider transactionLockProvider,
                         boolean synchronousCommit) {
    this.actionManager = actionManager;
    this.transactionLockProvider = transactionLockProvider;
    this.synchronousCommit = synchronousCommit;
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
    try {
      synchronousHappened(new TransactionCommitAction(handle));
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

  @Override
  public void happened(Action action) throws TransactionException, InterruptedException {
    Collection<Lock> locks = action.lock(transactionLockProvider);
    try {
      synchronousHappened(action);
    } finally {
      for (Lock lock : locks) {
        lock.unlock();
      }
    }
  }

  private void synchronousHappened(Action action) throws InterruptedException,
          TransactionException {
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
}
