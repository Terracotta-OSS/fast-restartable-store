/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
public class MockTransactionCommitAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionCommitAction(long id) {
    this.id = id;
  }

  @Override
  public long getPreviousLsn() {
    return -1;
  }

  @Override
  public void record(long lsn) {
    //
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  @Override
  public String toString() {
    return "Action: commitTransaction(" + id + ")";
  }

  public long getId() {
   return id;
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    return Collections.emptyList();
  }
}
