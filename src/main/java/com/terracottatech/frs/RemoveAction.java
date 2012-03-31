/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionLockProvider;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class RemoveAction implements Action {
  private final ObjectManager<Long, ByteBuffer, ?> objectManager;
  private final Long id;
  private final ByteBuffer key;

  RemoveAction(ObjectManager<Long, ByteBuffer, ?> objectManager, Long id, ByteBuffer key) {
    this.objectManager = objectManager;
    this.id = id;
    this.key = key;
  }

  @Override
  public long getPreviousLsn() {
    return objectManager.getLsn(id, key);
  }

  @Override
  public void record(long lsn) {
    objectManager.remove(id, key);
  }

  @Override
  public void replay(long lsn) {
    // Nothing to remove on replay
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForKey(id, key).writeLock();
    lock.lock();
    return Collections.singleton(lock);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RemoveAction that = (RemoveAction) o;

    if (id != null ? !id.equals(that.id) : that.id != null) return false;
    return !(key != null ? !key.equals(that.key) : that.key != null);
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    return result;
  }
}
