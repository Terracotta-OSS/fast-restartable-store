/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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

  @Before
  public void setUp() throws Exception {
    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong());
    filter = new TransactionFilter(delegate);
  }

  @Test
  public void testUnknownTransactions() throws Exception {
    // Add some transactions
    assertThat(filter.filter(transactionCommitAction(1), 10), is(true));
    assertThat(filter.filter(transactionCommitAction(2), 9), is(true));
    assertThat(filter.filter(transactionCommitAction(3), 8), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    // Try some transactional actions that aren't in the set
    assertThat(filter.filter(transactionalAction(4), 7), is(false));
    assertThat(filter.filter(transactionalAction(5), 8), is(false));
    assertThat(filter.filter(transactionalAction(6), 9), is(false));
    verify(delegate, never()).filter(any(Action.class), anyLong());
  }

  @Test
  public void testPassThrough() throws Exception {
    // Add some transactions
    assertThat(filter.filter(transactionCommitAction(1), 10), is(true));
    assertThat(filter.filter(transactionCommitAction(2), 9), is(true));
    assertThat(filter.filter(transactionCommitAction(3), 8), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    Action action = mock(Action.class);
    assertThat(filter.filter(action, 7), is(true));
    verify(delegate).filter(action, 7);
  }

  @Test
  public void testOpenTransaction() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    assertThat(filter.filter(transactionalAction(1), 2), is(true));
    verify(delegate).filter(any(Action.class), eq(2L));
  }

  @Test
  public void testClosedTransaction() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    assertThat(filter.filter(transactionBeginAction(1), 2), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    assertThat(filter.filter(transactionalAction(1), 1), is(false));
    verify(delegate, never()).filter(any(Action.class), anyLong());
  }

  private TransactionalAction transactionalAction(long id) {
    return new TransactionalAction(new TransactionHandleImpl(id), mock(Action.class));
  }

  private TransactionCommitAction transactionCommitAction(long id) {
    return new TransactionCommitAction(new TransactionHandleImpl(id));
  }

  private TransactionBeginAction transactionBeginAction(long id) {
    return new TransactionBeginAction(new TransactionHandleImpl(id));
  }
}
