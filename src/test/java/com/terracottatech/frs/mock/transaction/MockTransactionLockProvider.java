/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.terracottatech.frs.transaction.TransactionLockProvider;

public class MockTransactionLockProvider implements
    TransactionLockProvider {

  private final List<ReadWriteLock> idLocks = new ArrayList<ReadWriteLock>();
  private final List<ReadWriteLock> keyLocks = new ArrayList<ReadWriteLock>();
  
  public MockTransactionLockProvider(int idConcurrency, int keyConcurrency) {
    for (int i = 0; i < idConcurrency; i++) {
      idLocks.add(new ReentrantReadWriteLock());
    }
    for (int i = 0; i < keyConcurrency; i++) {
      keyLocks.add(new ReentrantReadWriteLock());
    }
  }
  
  @Override
  public ReadWriteLock getLockForKey(Object id, Object key) {
    ReadWriteLock idLock = getLockForId(id);
    ReadWriteLock keyLock = keyLocks.get(Math.abs(key.hashCode() % keyLocks.size()));
    return new KeyReadWriteLock(idLock, keyLock);
  }

  @Override
  public ReadWriteLock getLockForId(Object id) {
    return idLocks.get(Math.abs(id.hashCode() % idLocks.size()));
  }
  
  static class KeyReadWriteLock implements ReadWriteLock {

    private final Lock readLock;
    private final Lock writeLock;
    
    KeyReadWriteLock(ReadWriteLock idLock, ReadWriteLock keyLock) {
      readLock = new CompositeLock(idLock.readLock(), keyLock.readLock());
      writeLock = new CompositeLock(idLock.readLock(), keyLock.writeLock());
    }
    
    @Override
    public Lock readLock() {
      return readLock;
    }

    @Override
    public Lock writeLock() {
      return writeLock;
    }
  }
  
  static class CompositeLock implements Lock {
    private final Lock[] locks; 
    
    public CompositeLock(Lock ... locks) {
      this.locks = locks;
    }

    @Override
    public void lock() {
      for (Lock lock : locks) {
        lock.lock();
      }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      List<Lock> holds = new LinkedList<Lock>();
      for (Lock lock : locks) {
        if (lock.tryLock()) { 
          holds.add(lock);
        } else {
          for (Lock hold : holds) {
            hold.unlock();
          }
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)
        throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
      for (Lock lock : locks) {
        lock.unlock();
      }
    }
  }
}
