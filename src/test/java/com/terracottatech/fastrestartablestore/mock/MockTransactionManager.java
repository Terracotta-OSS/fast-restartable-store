/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author cdennis
 */
class MockTransactionManager implements TransactionManager<String, String> {

  private final AtomicLong txnId = new AtomicLong();
  
  private final RecordManager rcdManager;
  
  public MockTransactionManager(RecordManager rcdManager) {
    this.rcdManager = rcdManager;
  }

  public TransactionHandle create() {
    long id = txnId.getAndIncrement();
    rcdManager.asyncHappened(new MockTransactionBeginAction(id));
    return new MockTransactionHandle(id);
  }

  public void commit(TransactionHandle handle) {
    Future<Void> f = rcdManager.happened(new MockTransactionCommitAction(getIdAndValidateHandle(handle)));
    try {
      f.get();
    } catch (InterruptedException ex) {
      throw new AssertionError(ex);
    } catch (ExecutionException ex) {
      throw new AssertionError(ex);
    }
  }

  public void happened(TransactionHandle handle, Action action) {
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
