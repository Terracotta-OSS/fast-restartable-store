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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class TransactionManagerImplTest {
  private TransactionManager transactionManager;
  private ActionManager actionManager;
  private Future<Void> happenedFuture;

  @Before
  public void setUp() throws Exception {
    happenedFuture = mock(Future.class);
    actionManager = mock(ActionManager.class);
    when(actionManager.happened((Action) anyObject())).thenReturn(happenedFuture);
    transactionManager = new TransactionManagerImpl(actionManager, true);
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
    transactionManager.commit(handle);
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
  public void testHappened() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    Action action = mock(Action.class);
    transactionManager.happened(handle, action);
    verify(actionManager).asyncHappened(new TransactionalAction(handle, action));
    transactionManager.commit(handle);
    try {
      transactionManager.happened(handle, action);
      fail("Using a committed transaction handle should throw.");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}
