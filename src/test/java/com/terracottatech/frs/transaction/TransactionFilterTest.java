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
  private Action action;

  @Before
  public void setUp() throws Exception {
    delegate = mock(Filter.class);
    doReturn(true).when(delegate).filter(any(Action.class), anyLong());
    filter = new TransactionFilter(delegate);
    action = mock(Action.class);
  }

  @Test
  public void testUnknownTransactions() throws Exception {
    // Add some transactions
    assertThat(filter.filter(transactionCommitAction(1), 10), is(true));
    assertThat(filter.filter(transactionCommitAction(2), 9), is(true));
    assertThat(filter.filter(transactionCommitAction(3), 8), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    // Try some transactional actions that aren't in the set
    assertThat(filter.filter(transactionalAction(4, false), 7), is(false));
    assertThat(filter.filter(transactionalAction(5, false), 8), is(false));
    assertThat(filter.filter(transactionalAction(6, false), 9), is(false));
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

    assertThat(filter.filter(transactionalAction(1, false), 2), is(true));
    verify(delegate).filter(any(Action.class), eq(2L));
  }

  @Test
  public void testClosedTransaction() throws Exception {
    assertThat(filter.filter(transactionCommitAction(1), 3), is(true));
    verify(delegate, never()).filter(any(Action.class), anyLong());

    assertThat(filter.filter(transactionalAction(1, true), 2), is(true));
    verify(delegate).filter(action, 2L);

    assertThat(filter.filter(transactionalAction(1, false), 1), is(false));
    verify(delegate, never()).filter(action, 1L);
  }

  private TransactionalAction transactionalAction(long id, boolean begin) {
    return new TransactionalAction(new TransactionHandleImpl(id), begin, false, action, null);
  }

  private TransactionCommitAction transactionCommitAction(long id) {
    return new TransactionCommitAction(new TransactionHandleImpl(id), false);
  }
}
