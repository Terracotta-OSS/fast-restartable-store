/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class TransactionalAction implements Action {
  private final TransactionHandle handle;
  private final Action action;

  TransactionalAction(TransactionHandle handle, Action action) {
    this.handle = handle;
    this.action = action;
  }

  TransactionHandle getHandle() {
    return handle;
  }

  Action getAction() {
    return action;
  }

  @Override
  public long getPreviousLsn() {
    return action.getPreviousLsn();
  }

  @Override
  public void record(long lsn) {
    action.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    action.replay(lsn);
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return action.lock(lockProvider);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionalAction that = (TransactionalAction) o;

    if (action != null ? !action.equals(that.action) : that.action != null) return false;
    return !(handle != null ? !handle.equals(that.handle) : that.handle != null);
  }

  @Override
  public int hashCode() {
    int result = handle != null ? handle.hashCode() : 0;
    result = 31 * result + (action != null ? action.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TransactionalAction{" +
            "handle=" + handle +
            ", action=" + action +
            '}';
  }
}
