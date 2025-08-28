/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.compaction.SecureCompactorImpl;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.flash.ReadManager;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;

/**
 * A secure implementation of RestartStore, that extends RestartStoreImpl, which enforces that put
 * operations use encryption
 */
public class SecureRestartStoreImpl extends RestartStoreImpl {
  private final CipherManager cipherManager;

  public SecureRestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      TransactionManager transactionManager, LogManager logManager, ActionManager actionManager,
      ReadManager read, Compactor compactor, Configuration configuration,
      CipherManager cipherManager) {
    super(objectManager, transactionManager, logManager, actionManager, read, compactor,
        configuration);
    this.cipherManager = cipherManager;
  }

  public SecureRestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
      TransactionManager transactionManager, LogManager logManager, ActionManager actionManager,
      ReadManager read, IOManager ioManager, Configuration configuration,
      CipherManager cipherManager) throws RestartStoreException {
    this(objectManager, transactionManager, logManager, actionManager, read,
        new SecureCompactorImpl(objectManager, transactionManager, logManager, ioManager,
            configuration, actionManager, cipherManager),
        configuration, cipherManager);
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginTransaction(boolean synchronous) {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> baseTransaction =
        super.beginTransaction(synchronous);
    return new SecureTransaction(baseTransaction);
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginAutoCommitTransaction(
      boolean synchronous) {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> baseTransaction =
        super.beginAutoCommitTransaction(synchronous);
    return new SecureTransaction(baseTransaction);
  }

  /**
   * A wrapper around a Transaction that enforces encryption.
   */
  private class SecureTransaction implements Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delegate;

    public SecureTransaction(Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delegate) {
      this.delegate = delegate;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(PutAction action)
        throws TransactionException {
      if (!(action instanceof SecurePutAction)) {
        throw new IllegalArgumentException(
            "Only SecurePutAction is supported in SecureTransaction");
      }
      delegate.put(action);
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id,
        ByteBuffer key, ByteBuffer value) throws TransactionException {
      delegate.put(new SecurePutAction(getObjectManager(), getCompactor(), cipherManager, id, key,
          value, isRecovering()));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id)
        throws TransactionException {
      delegate.delete(id);
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id,
        ByteBuffer key) throws TransactionException {
      delegate.remove(id, key);
      return this;
    }

    @Override
    public synchronized void commit() throws TransactionException {
      delegate.commit();
    }
  }
}
