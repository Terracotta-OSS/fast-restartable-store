/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Transaction;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;

/**
 *
 * @author cdennis
 */
class MockTransaction implements Transaction<Long, String, String> {

  private final TransactionManager txnManager;
  private final TransactionHandle txnHandle;
  
  public MockTransaction(TransactionManager txnManager) {
    this.txnManager = txnManager;
    this.txnHandle = txnManager.create();
  }

  public void put(Long id, String key, String value) {
    txnManager.happened(txnHandle, new MockPutAction<Long, String, String>(id, key, value));
  }

  public void remove(Long id, String key) {
    txnManager.happened(txnHandle, new MockRemoveAction<Long, String>(id, key));
  }

  public void delete(Long id) {
    txnManager.happened(txnHandle, new MockDeleteAction<Long>(id));
  }

  public void commit() {
    txnManager.commit(txnHandle);
  }
  
}
