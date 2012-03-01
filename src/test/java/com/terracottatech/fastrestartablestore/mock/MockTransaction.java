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
class MockTransaction implements Transaction<String, String> {

  private final TransactionManager txnManager;
  private final TransactionHandle txnHandle;
  
  public MockTransaction(TransactionManager txnManager) {
    this.txnManager = txnManager;
    this.txnHandle = txnManager.create();
  }

  public void put(String key, String value) {
    txnManager.happened(txnHandle, new MockPutAction<String, String>(key, value));
  }

  public void remove(String key) {
    txnManager.happened(txnHandle, new MockRemoveAction<String>(key));
  }

  public void commit() {
    txnManager.commit(txnHandle);
  }
  
}
