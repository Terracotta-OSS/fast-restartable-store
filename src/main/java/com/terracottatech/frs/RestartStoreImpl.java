/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

/**
 *
 * @author twu
 */
class RestartStoreImpl<I, K, V> implements RestartStore<I, K, V> {
  private final ObjectManager<I, K, V> objectManager;
  private final TransactionManager transactionManager;

  RestartStoreImpl(ObjectManager<I, K, V> objectManager, TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    this.objectManager = objectManager;
  }

  @Override
  public Transaction<I, K, V> beginTransaction() {
    return new TransactionImpl();
  }

  private class TransactionImpl implements Transaction<I, K, V> {
    private final TransactionHandle handle;
    private boolean committed = false;

    TransactionImpl() {
      this.handle = transactionManager.begin();
    }

    @Override
    public synchronized Transaction<I, K, V> put(I id, K key, V value) {
      checkCommitted();
      transactionManager.happened(handle, new PutAction<I, K, V>(objectManager, id, key, value));
      return this;
    }

    @Override
    public synchronized Transaction<I, K, V> delete(I id) {
      checkCommitted();
      transactionManager.happened(handle, new DeleteAction<I>(objectManager, id));
      return this;
    }

    @Override
    public synchronized Transaction<I, K, V> remove(I id, K key) {
      checkCommitted();
      transactionManager.happened(handle, new RemoveAction<I, K>(objectManager, id, key));
      return this;
    }

    @Override
    public synchronized void commit() {
      checkCommitted();
      transactionManager.commit(handle);
      committed = true;
    }

    private void checkCommitted() {
      if (committed) throw new IllegalStateException("Transaction is already committed.");
    }
  }
}
