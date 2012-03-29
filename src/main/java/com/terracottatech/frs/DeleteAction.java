/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionLockProvider;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class DeleteAction<I> implements Action {
  private final ObjectManager<I, ?, ?> objectManager;
  private final I id;

  DeleteAction(ObjectManager<I, ?, ?> objectManager, I id) {
    this.objectManager = objectManager;
    this.id = id;
  }

  @Override
  public long getPreviousLsn() {
    return 0;
  }

  @Override
  public void record(long lsn) {
    objectManager.delete(id);
  }

  @Override
  public void replay(long lsn) {
    // nothing to do on replay
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForId(id).writeLock();
    lock.lock();
    return Collections.singleton(lock);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DeleteAction that = (DeleteAction) o;

    return !(id != null ? !id.equals(that.id) : that.id != null);
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
