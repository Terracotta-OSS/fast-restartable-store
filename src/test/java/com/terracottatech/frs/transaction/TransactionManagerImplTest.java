/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.NullActionManager;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class TransactionManagerImplTest {
  private TransactionManager transactionManager;
  private TxnManagerTestActionManager actionManager;
  private Future<Void> happenedFuture;
  private Action action;
  private TransactionLSNCallback callback;

  @Before
  public void setUp() throws Exception {
    happenedFuture = mock(Future.class);
    action = mock(Action.class);
    actionManager = spy(new TxnManagerTestActionManager());
    doReturn(happenedFuture).when(actionManager).syncHappened(any(Action.class));
    doReturn(happenedFuture).when(actionManager).happened(any(Action.class));
    callback = mock(TransactionLSNCallback.class);
    transactionManager = new TransactionManagerImpl(actionManager, true);
  }

  @Test
  public void testBegin() throws Exception {
    transactionManager.begin();
    verify(actionManager, never()).asyncHappened(any(Action.class));
  }

  @Test
  public void testCommit() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    transactionManager.commit(handle);
    verify(actionManager).syncHappened(
            new TransactionCommitAction(handle, false));
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
    TransactionManager asyncCommitManager = new TransactionManagerImpl(actionManager, false);
    TransactionHandle handle = asyncCommitManager.begin();
    asyncCommitManager.commit(handle);
    verify(actionManager).asyncHappened(
            new TransactionCommitAction(handle, true));
  }

  @Test
  public void testHappened() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    verify(actionManager).asyncHappened(new TransactionalAction(handle, true, false, action, callback));
    transactionManager.happened(handle, action);
    verify(actionManager).asyncHappened(new TransactionalAction(handle, false, false, action, callback));
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
    verify(actionManager).syncHappened(action);
    verify(happenedFuture).get();
  }

  @Test
  public void testAsyncAutocommit() throws Exception {
    TransactionManager asyncTransactionManager =
            new TransactionManagerImpl(actionManager, false);
    asyncTransactionManager.happened(action);
    verify(actionManager).asyncHappened(action);
  }

  @Test
  public void testLowestLsn() throws Exception {
    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(Long.MAX_VALUE));

    TransactionHandle txn1 = transactionManager.begin();
    TransactionHandle txn2 = transactionManager.begin();

    transactionManager.happened(txn1, action);
    transactionManager.happened(txn2, action);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(0L));

    transactionManager.commit(txn1);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(1L));

    transactionManager.commit(txn2);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(Long.MAX_VALUE));
  }

  private class TxnManagerTestActionManager extends NullActionManager {
    long lsn = 0;
    @Override
    public void asyncHappened(Action action) {
      action.record(lsn++);
    }
  }
}
