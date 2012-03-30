/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

/**
 *
 * @author cdennis
 */
class MockTransaction implements Transaction<Long, String, String> {

  private final TransactionManager txnManager;
  private final ObjectManager<Long, String, String> objManager;
  private final TransactionHandle txnHandle;
  
  public MockTransaction(TransactionManager txnManager, ObjectManager<Long, String, String> objManager) {
    this.txnManager = txnManager;
    this.objManager = objManager;
    this.txnHandle = txnManager.begin();
  }

  @Override
  public Transaction<Long, String, String> put(Long id, String key, String value) {
    txnManager.happened(txnHandle, new MockPutAction<Long, String, String>(objManager, id, key, value));
    return this;
  }

  @Override
  public Transaction<Long, String, String> remove(Long id, String key) {
    txnManager.happened(txnHandle, new MockRemoveAction<Long, String>(objManager, id, key));
    return this;
  }

  @Override
  public Transaction<Long, String, String> delete(Long id) {
    txnManager.happened(txnHandle, new MockDeleteAction<Long>(objManager, id));
    return this;
  }

  @Override
  public void commit() throws InterruptedException, TransactionException {
    txnManager.commit(txnHandle);
  }
  
}
