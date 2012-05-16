/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.mock.recovery.MockAbstractFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.action.Action;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockTransactionFilter extends MockAbstractFilter<Action, Action> {

  private final Set<Long> validTransactions = new HashSet<Long>();

  public MockTransactionFilter(Filter<Action> next) {
    super(next);
  }
  
  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof MockTransactionCommitAction) {
      validTransactions.add(((MockTransactionCommitAction) element).getId());
      return true;
    } else if (element instanceof MockTransactionBeginAction) {
      validTransactions.remove(((MockTransactionBeginAction) element).getId());
      return true;
    } else if (element instanceof MockTransactionalAction && !validTransactions.contains(((MockTransactionalAction) element).getId())) {
      return delegate(element, lsn, true);
    } else {
      return delegate(element, lsn, filtered);
    }
  }

  @Override
  protected Action convert(Action element) {
    if (element instanceof MockTransactionalAction) {
      return ((MockTransactionalAction) element).getEmbeddedAction();
    } else {
      return element;
    }
  }
  
}
