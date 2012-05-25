/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.compaction.CompactorImpl;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.recovery.RecoveryException;
import com.terracottatech.frs.recovery.RecoveryListener;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.recovery.RecoveryManagerImpl;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author twu
 */
public class RestartStoreImpl implements RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>,
        RecoveryListener {
  private enum State {
    INIT, RECOVERING, RUNNING, SHUTDOWN
  }

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager                                transactionManager;
  private final Compactor compactor;
  private final LogManager logManager;
  private final ActionManager actionManager;
  private final Configuration configuration;

  private volatile State state = State.INIT;

  RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                   TransactionManager transactionManager, LogManager logManager,
                   ActionManager actionManager, Compactor compactor,
                   Configuration configuration) {
    this.transactionManager = transactionManager;
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.compactor = compactor;
    this.configuration = configuration;
  }

  public RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                          TransactionManager transactionManager, LogManager logManager,
                          ActionManager actionManager, IOManager ioManager,
                          Configuration configuration) throws RestartStoreException {
    this(objectManager, transactionManager, logManager, actionManager,
         new CompactorImpl(objectManager, transactionManager, logManager, ioManager, configuration,
                           actionManager),
         configuration);
  }

  @Override
  public synchronized Future<Void> startup() throws InterruptedException,
          RecoveryException {
    if (state != State.INIT && state != State.SHUTDOWN) {
      throw new IllegalStateException("Can't startup from state " + state);
    }
    state = State.RECOVERING;
    RecoveryManager recoveryManager = new RecoveryManagerImpl(logManager, actionManager,
                                                              configuration);
    return recoveryManager.recover(this);
  }

  @Override
  public synchronized void recovered() {
    if (state == State.RECOVERING) {
      compactor.startup();
      state = State.RUNNING;
    }
  }

  @Override
  public synchronized void shutdown() throws InterruptedException {
    if (state != State.SHUTDOWN) {
      state = State.SHUTDOWN;
      compactor.shutdown();
      logManager.shutdown();
    }
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginTransaction(boolean synchronous) {
    checkReadyState();
    return new TransactionImpl(synchronous);
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginAutoCommitTransaction(boolean synchronous) {
    checkReadyState();
    return new AutoCommitTransaction(synchronous);
  }

  private void checkReadyState() {
    if (state != State.RUNNING && state != State.RECOVERING) {
      throw new IllegalStateException("RestartStore is not ready for mutations.");
    }
  }

  private boolean isRecovering() {
    return state == State.RECOVERING;
  }

  private class AutoCommitTransaction implements
          Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final boolean synchronous;

    private AutoCommitTransaction(boolean synchronous) {
      this.synchronous = synchronous;
    }

    private void happened(Action action) throws TransactionException,
            InterruptedException {
      if (synchronous) {
        try {
          actionManager.syncHappened(action).get();
        } catch (ExecutionException e) {
          throw new TransactionException(e);
        }
      } else {
        actionManager.happened(action);
      }
    }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) throws
            TransactionException, InterruptedException {
      checkReadyState();
      happened(new PutAction(objectManager, compactor, id, key, value, isRecovering()));
      return this;
  }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) throws
            TransactionException, InterruptedException {
      checkReadyState();
      happened(new DeleteAction(objectManager, compactor, id, isRecovering()));
      return this;
    }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) throws
            TransactionException, InterruptedException {
      checkReadyState();
      happened(new RemoveAction(objectManager, compactor, id, key, isRecovering()));
      return this;
    }

    @Override
    public void commit() throws InterruptedException, TransactionException {
    }
  }

  private class TransactionImpl implements
          Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final boolean synchronous;
    private final TransactionHandle handle;
    private boolean committed = false;

    TransactionImpl(boolean synchronous) {
      this.handle = transactionManager.begin();
      this.synchronous = synchronous;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new PutAction(objectManager, compactor, id, key, value, isRecovering()));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new DeleteAction(objectManager, compactor, id, isRecovering()));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new RemoveAction(objectManager, compactor, id, key, isRecovering()));
      return this;
    }

    @Override
    public synchronized void commit() throws InterruptedException, TransactionException {
      checkReadyState();
      checkCommitted();
      transactionManager.commit(handle, synchronous);
      committed = true;
    }

    private void checkCommitted() {
      if (committed) throw new IllegalStateException("Transaction is already committed.");
    }
  }
}
