/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.Filter;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class TransactionFilterTest {
  private Filter<Action> delegate;
  private TransactionFilter filter;
  private Action action;

  @Before
  public void setUp() throws Exception {
    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong(), eq(false));
    doReturn(false).when(delegate).filter(any(Action.class), anyLong(), eq(true));
    filter = new TransactionFilter(delegate);
    action = mock(Action.class);
  }

  @Test
  public void testSkippedTransactionBegin() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3, false), is(true));
    // action is skipped, but the transaction begin is still processed.
    assertThat(filter.filter(transactionalAction(1, true), 2, true), is(false));
    // Since the action belongs to a now closed transaction, it should be filtered.
    assertThat(filter.filter(transactionalAction(1, false), 1, false), is(false));
  }

  @Test
  public void testUnknownTransactions() throws Exception {
    // Add some transactions
    assertThat(filter.filter(transactionCommitAction(1), 10, false), is(true));
    assertThat(filter.filter(transactionCommitAction(2), 9, false), is(true));
    assertThat(filter.filter(transactionCommitAction(3), 8, false), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong(), anyBoolean());

    // Try some transactional actions that aren't in the set
    assertThat(filter.filter(transactionalAction(4, false), 7, false), is(false));
    verify(delegate).filter(any(Action.class), eq(7L), eq(true));
    assertThat(filter.filter(transactionalAction(5, false), 8, false), is(false));
    verify(delegate).filter(any(Action.class), eq(8L), eq(true));
    assertThat(filter.filter(transactionalAction(6, false), 9, false), is(false));
    verify(delegate).filter(any(Action.class), eq(9L), eq(true));
  }

  @Test
  public void testPassThrough() throws Exception {
    // Add some transactions
    assertThat(filter.filter(transactionCommitAction(1), 10, false), is(true));
    assertThat(filter.filter(transactionCommitAction(2), 9, false), is(true));
    assertThat(filter.filter(transactionCommitAction(3), 8, false), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong(), anyBoolean());

    Action action = mock(Action.class);
    assertThat(filter.filter(action, 7, false), is(true));
    verify(delegate).filter(action, 7, false);
  }

  @Test
  public void testOpenTransaction() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3, false), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong(), anyBoolean());

    assertThat(filter.filter(transactionalAction(1, false), 2, false), is(true));
    verify(delegate).filter(any(Action.class), eq(2L), eq(false));
  }

  @Test
  public void testClosedTransaction() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3, false), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong(), anyBoolean());

    assertThat(filter.filter(transactionalAction(1, true), 2, false), is(true));
    verify(delegate).filter(action, 2L, false);

    assertThat(filter.filter(transactionalAction(1, false), 1, false), is(false));
    verify(delegate, never()).filter(action, 1L, false);
  }

  private TransactionalAction transactionalAction(long id, boolean begin) {
    return new TransactionalAction(new TransactionHandleImpl(id), begin, false, action, null);
  }

  private TransactionCommitAction transactionCommitAction(long id) {
    return new TransactionCommitAction(new TransactionHandleImpl(id), false);
  }
}
