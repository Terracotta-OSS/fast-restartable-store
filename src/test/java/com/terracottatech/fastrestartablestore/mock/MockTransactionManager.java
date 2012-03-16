/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
class MockTransactionManager implements TransactionManager {

  private final AtomicLong txnId = new AtomicLong();
  
  private final RecordManager rcdManager;
  
  private final Map<TransactionHandle, Collection<Lock>> heldLocks = new ConcurrentHashMap<TransactionHandle, Collection<Lock>>();
  private final TransactionLockProvider locks;
  
  public MockTransactionManager(RecordManager rcdManager) {
    this.rcdManager = rcdManager;
    locks = new MockTransactionLockProvider(1024, 1024);
  }

  public TransactionHandle begin() {
    long id = txnId.getAndIncrement();
    rcdManager.asyncHappened(new MockTransactionBeginAction(id));
    TransactionHandle handle = new MockTransactionHandle(id);
    heldLocks.put(handle, new ArrayList<Lock>());
    return handle;
  }

  public void commit(TransactionHandle handle) {
    Future<Void> f = rcdManager.happened(new MockTransactionCommitAction(getIdAndValidateHandle(handle)));
    try {
      f.get();
    } catch (InterruptedException ex) {
      throw new AssertionError(ex);
    } catch (ExecutionException ex) {
      throw new AssertionError(ex);
    } finally {
      for (Lock l : heldLocks.remove(handle)) {
        l.unlock();
      }
    }
  }

  public void happened(TransactionHandle handle, Action action) {
    heldLocks.get(handle).addAll(action.lock(locks));
    rcdManager.happened(new MockTransactionalAction(getIdAndValidateHandle(handle), action));
  }

  private long getIdAndValidateHandle(TransactionHandle handle) {
    if (handle instanceof MockTransactionHandle) {
      MockTransactionHandle mth = (MockTransactionHandle) handle;
      return mth.getId(this);
    } else {
      throw new IllegalArgumentException("Not one of our handles");
    }
  }
  
  private class MockTransactionHandle implements TransactionHandle {

    private final long id;
    
    public MockTransactionHandle(long id) {
      this.id = id;
    }
    
    private long getId(MockTransactionManager asker) {
      if (MockTransactionManager.this != asker) {
        throw new IllegalArgumentException("Not my owner");
      } else {
        return id;
      }
    }
  }
}
