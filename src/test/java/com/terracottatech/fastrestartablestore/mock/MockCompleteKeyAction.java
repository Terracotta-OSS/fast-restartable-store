/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author cdennis
 */
public abstract class MockCompleteKeyAction<I, K> implements Action {
  private final I id;
  private final K key;
  
  public MockCompleteKeyAction(I id, K key) {
    this.id = id;
    this.key = key;
  }
  
  protected I getId() {
    return id;
  }

  protected K getKey() {
    return key;
  }

  @Override
  public final Collection<Lock> lock(List<ReadWriteLock> locks) {
    int idLockIndex = Math.abs(getId().hashCode() % locks.size());
    int keyLockIndex = Math.abs(getKey().hashCode() % locks.size());
    Lock idLock = locks.get(idLockIndex).readLock();
    Lock keyLock = locks.get(keyLockIndex).writeLock();
    
    if (idLockIndex < keyLockIndex) {
      idLock.lock();
      keyLock.lock();
    } else {
      keyLock.lock();
      idLock.lock();
    }
    return Arrays.asList(idLock, keyLock);
  }
}
