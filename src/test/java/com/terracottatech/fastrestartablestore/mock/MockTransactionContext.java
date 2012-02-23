/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.TransactionContext;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;

/**
 *
 * @author cdennis
 */
class MockTransactionContext implements TransactionContext<String, String> {

  private final TransactionManager<String, String> txnManager;
  private final TransactionHandle txnHandle;
  
  public MockTransactionContext(TransactionManager txnManager) {
    this.txnManager = txnManager;
    this.txnHandle = txnManager.create();
  }

  public void put(String key, String value) {
    txnManager.happened(txnHandle, new MockPutAction(key, value));
  }

  public void remove(String key) {
    txnManager.happened(txnHandle, new MockRemoveAction(key));
  }

  public void commit() {
    txnManager.commit(txnHandle);
  }
  
}
