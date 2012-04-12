/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.AbstractAdaptingFilter;
import com.terracottatech.frs.recovery.Filter;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class TransactionFilter extends AbstractAdaptingFilter<Action, Action> {
  private final Set<TransactionHandle> openTransactions =
          new HashSet<TransactionHandle>();

  public TransactionFilter(Filter<Action> nextFilter) {
    super(nextFilter);
  }

  @Override
  protected Action convert(Action element) {
    if (element instanceof TransactionalAction) {
      return ((TransactionalAction) element).getAction();
    } else {
      return element;
    }
  }

  @Override
  public boolean filter(Action element, long lsn) {
    if (element instanceof TransactionCommitAction) {
      openTransactions.add(((TransactionCommitAction) element).getHandle());
      return true;
    } else if (element instanceof TransactionBeginAction) {
      openTransactions.remove(((TransactionBeginAction) element).getHandle());
      return true;
    } else if (element instanceof TransactionalAction) {
      if (openTransactions.contains(((TransactionalAction) element).getHandle())) {
        return delegate(element, lsn);
      } else {
        return false;
      }
    } else {
      return delegate(element, lsn);
    }
  }
}
