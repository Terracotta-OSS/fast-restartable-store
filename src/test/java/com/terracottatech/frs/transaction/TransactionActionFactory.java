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

  public TransactionHandle transactionHandle(long id) {
    return new TransactionHandleImpl(id);
  }

  public Action transactionBegin(long id) {
    return new TransactionBeginAction(transactionHandle(id));
  }

  public Action transactionCommit(long id) {
    return new TransactionCommitAction(transactionHandle(id));
  }

  public Action transactionalAction(long id, Action action) {
    return new TransactionalAction(transactionHandle(id), action);
  }
}
