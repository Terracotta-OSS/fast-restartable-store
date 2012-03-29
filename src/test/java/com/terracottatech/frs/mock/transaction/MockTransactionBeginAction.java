/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.action.Action;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author cdennis
 */
public class MockTransactionBeginAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionBeginAction(long id) {
    this.id = id;
  }
  
  @Override
  public long getPreviousLsn() {
    return -1;
  }

  @Override
  public void record(long lsn) {
    // Nothing to do
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  public String toString() {
    return "Action: beginTransaction(" + id + ")";
  }

  public Collection<Lock> lock(TransactionLockProvider locks) {
    return Collections.emptyList();
  }

  public long getId() {
    return id;
  }
}
