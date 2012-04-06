/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.action.Action;

/**
 *
 * @author cdennis
 */
public class MockTransactionManager implements TransactionManager {

  private final AtomicLong txnId = new AtomicLong();
  
  private final ActionManager rcdManager;
  
  private final Map<TransactionHandle, Collection<Lock>> heldLocks = new ConcurrentHashMap<TransactionHandle, Collection<Lock>>();
  private final TransactionLockProvider locks;
  
  public MockTransactionManager(ActionManager rcdManager) {
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

    @Override
    public ByteBuffer toByteBuffer() {
      return null;
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
