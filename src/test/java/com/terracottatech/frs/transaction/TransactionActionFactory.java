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
  private static final TransactionLSNCallback NULL_CALLBACK = new TransactionLSNCallback() {
    @Override
    public void setLsn(long lsn) {
    }
  };

  public TransactionHandle transactionHandle(long id) {
    return new TransactionHandleImpl(id);
  }

  public Action transactionCommit(long id) {
    return new TransactionCommitAction(transactionHandle(id), false);
  }

  public Action transactionalAction(long id, Action action, boolean begin) {
    return new TransactionalAction(transactionHandle(id), begin, false, action, NULL_CALLBACK);
  }
}
