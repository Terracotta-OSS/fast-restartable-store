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
class PutAction implements Action {
  private final ObjectManager<Long, ByteBuffer, ByteBuffer> objectManager;
  private final Long id;
  private final ByteBuffer key;
  private final ByteBuffer value;

  PutAction(ObjectManager<Long, ByteBuffer, ByteBuffer> objectManager, Long id, ByteBuffer key, ByteBuffer value) {
    this.objectManager = objectManager;
    this.id = id;
    this.key = key;
    this.value = value;
  }

  @Override
  public long getPreviousLsn() {
    return objectManager.getLsn(id, key);
  }

  @Override
  public void record(long lsn) {
    objectManager.put(id, key, value, lsn);
  }

  @Override
  public void replay(long lsn) {
    objectManager.replayPut(id, key, value, lsn);
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

    PutAction putAction = (PutAction) o;

    if (id != null ? !id.equals(putAction.id) : putAction.id != null) return false;
    if (key != null ? !key.equals(putAction.key) : putAction.key != null) return false;
    return !(value != null ? !value.equals(putAction.value) : putAction.value != null);
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
