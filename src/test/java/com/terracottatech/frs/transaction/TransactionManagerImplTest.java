/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    callback = mock(TransactionLSNCallback.class);
    transactionManager = new TransactionManagerImpl(actionManager);
  }

  @Test
  public void testBegin() throws Exception {
    transactionManager.begin();
    verify(actionManager, never()).happened(any(Action.class));
  }

  @Test
  public void testCommit() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    transactionManager.commit(handle, true);
    verify(actionManager).syncHappened(
            new TransactionCommitAction(handle, false));
    try {
      transactionManager.commit(handle, true);
      fail("Committing a handle twice should fail.");
    } catch (IllegalArgumentException e) {
      // Expected
    }
    handle = transactionManager.begin();
    when(happenedFuture.get()).thenThrow(new ExecutionException(new Exception()));
    try {
      transactionManager.commit(handle, true);
      fail("Commit should have failed.");
    } catch (TransactionException e) {
      // Expected
    }
  }

  @Test
  public void testAsyncCommit() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.commit(handle, false);
    verify(actionManager).happened(
            new TransactionCommitAction(handle, true));
  }

  @Test
  public void testHappened() throws Exception {
    TransactionHandle handle = transactionManager.begin();
    transactionManager.happened(handle, action);
    verify(actionManager).happened(new TransactionalAction(handle, true, false, action, callback));
    transactionManager.happened(handle, action);
    verify(actionManager).happened(new TransactionalAction(handle, false, false, action, callback));
    transactionManager.commit(handle, true);
    try {
      transactionManager.happened(handle, action);
      fail("Using a committed transaction handle should throw.");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testLowestLsn() throws Exception {
    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(Long.MAX_VALUE));

    TransactionHandle txn1 = transactionManager.begin();
    TransactionHandle txn2 = transactionManager.begin();

    transactionManager.happened(txn1, action);
    transactionManager.happened(txn2, action);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(0L));

    transactionManager.commit(txn1, true);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(1L));

    transactionManager.commit(txn2, true);

    assertThat(transactionManager.getLowestOpenTransactionLsn(), is(Long.MAX_VALUE));
  }

  private class TxnManagerTestActionManager extends NullActionManager {
    long lsn = 0;
    @Override
    public Future<Void> happened(Action action) {
      action.record(lsn++);
      return happenedFuture;
    }
  }
}
