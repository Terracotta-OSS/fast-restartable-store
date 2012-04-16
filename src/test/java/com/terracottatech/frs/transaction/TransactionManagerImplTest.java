/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class TransactionManagerImplTest {
  private TransactionManager transactionManager;
  private TransactionLockProvider transactionLockProvider;
  private ActionManager actionManager;
  private Future<Void> happenedFuture;
  private Action action;
  private Lock lock;

  @Before
  public void setUp() throws Exception {
    happenedFuture = mock(Future.class);
    lock = mock(Lock.class);
    action = mock(Action.class);
    when(action.lock(any(TransactionLockProvider.class))).thenReturn(Collections.singleton(lock));
    actionManager = mock(ActionManager.class);
    when(actionManager.happened(any(Action.class))).thenReturn(happenedFuture);
    transactionLockProvider = mock(TransactionLockProvider.class);
    transactionManager = new TransactionManagerImpl(actionManager, transactionLockProvider, true);
  }

  @Test
  public void testBegin() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    verify(actionManager).asyncHappened(new TransactionBeginAction(handle));
    handle = transactionManager.begin();
    verify(actionManager).asyncHappened(new TransactionBeginAction(handle));
  }

  @Test
  public void testCommit() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    transactionManager.commit(handle);
    verify(lock).unlock();
    verify(actionManager).happened(new TransactionCommitAction(handle));
    try {
      transactionManager.commit(handle);
      fail("Committing a handle twice should fail.");
    } catch (IllegalArgumentException e) {
      // Expected
    }
    handle = transactionManager.begin();
    when(happenedFuture.get()).thenThrow(new ExecutionException(new Exception()));
    try {
      transactionManager.commit(handle);
      fail("Commit should have failed.");
    } catch (TransactionException e) {
      // Expected
    }
  }

  @Test
  public void testAsyncCommit() throws Exception {
    TransactionManager asyncCommitManager =
            new TransactionManagerImpl(actionManager, transactionLockProvider, false);
    TransactionHandle handle = asyncCommitManager.begin();
    asyncCommitManager.commit(handle);
    verify(actionManager).asyncHappened(new TransactionCommitAction(handle));
  }

  @Test
  public void testHappened() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    verify(action).lock(transactionLockProvider);
    verify(actionManager).asyncHappened(new TransactionalAction(handle, action));
    transactionManager.commit(handle);
    try {
      transactionManager.happened(handle, action);
      fail("Using a committed transaction handle should throw.");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testSynchronousAutoCommit() throws Exception {
    transactionManager.happened(action);
    verify(action).lock(transactionLockProvider);
    verify(actionManager).happened(action);
    verify(happenedFuture).get();
    verify(lock).unlock();
  }

  @Test
  public void testAsyncAutocommit() throws Exception {
    TransactionManager asyncTransactionManager =
            new TransactionManagerImpl(actionManager, transactionLockProvider, false);
    asyncTransactionManager.happened(action);
    verify(action).lock(transactionLockProvider);
    verify(actionManager).asyncHappened(action);
    verify(lock).unlock();
  }
}
