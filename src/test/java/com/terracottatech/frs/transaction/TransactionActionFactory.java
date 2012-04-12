/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

/**
 * @author tim
 */
public class TransactionActionFactory {
  public TransactionActionFactory() {}

  public Action transactionBegin(long id) {
    return new TransactionBeginAction(new TransactionHandleImpl(id));
  }

  public Action transactionCommit(long id) {
    return new TransactionCommitAction(new TransactionHandleImpl(id));
  }

  public Action transactionalAction(long id, Action action) {
    return new TransactionalAction(new TransactionHandleImpl(id), action);
  }
}
