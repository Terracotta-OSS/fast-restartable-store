/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
public class TransactionBeginAction implements Action {
  private final TransactionHandle handle;

  public TransactionBeginAction(TransactionHandle handle) {
    this.handle = handle;
  }

  TransactionHandle getHandle() {
    return handle;
  }

  @Override
  public long getPreviousLsn() {
    return 0;
  }

  @Override
  public void record(long lsn) {

  }

  @Override
  public void replay(long lsn) {

  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return Collections.emptySet();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionBeginAction that = (TransactionBeginAction) o;

    return !(handle != null ? !handle.equals(that.handle) : that.handle != null);
  }

  @Override
  public int hashCode() {
    return handle != null ? handle.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TransactionBeginAction{" +
            "handle=" + handle +
            '}';
  }
}
