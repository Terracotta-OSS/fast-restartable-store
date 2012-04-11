/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;

/**
 *
 * @author twu
 */
class RestartStoreImpl implements RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> {
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager transactionManager;

  RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    this.objectManager = objectManager;
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginTransaction() {
    return new TransactionImpl();
  }

  private class TransactionImpl implements Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final TransactionHandle handle;
    private boolean committed = false;

    TransactionImpl() {
      this.handle = transactionManager.begin();
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) {
      checkCommitted();
      transactionManager.happened(handle, new PutAction(objectManager, id, key, value));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) {
      checkCommitted();
      transactionManager.happened(handle, new DeleteAction(objectManager, id));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) {
      checkCommitted();
      transactionManager.happened(handle, new RemoveAction(objectManager, id, key));
      return this;
    }

    @Override
    public synchronized void commit() throws InterruptedException, TransactionException {
      checkCommitted();
      transactionManager.commit(handle);
      committed = true;
    }

    private void checkCommitted() {
      if (committed) throw new IllegalStateException("Transaction is already committed.");
    }
  }
}
