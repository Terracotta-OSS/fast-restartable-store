/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.compaction.CompactorImpl;
import com.terracottatech.frs.compaction.LSNGapCompactionPolicy;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.recovery.RecoveryListener;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.recovery.RecoveryManagerImpl;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

import java.nio.ByteBuffer;
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
  private final Transaction<ByteBuffer, ByteBuffer, ByteBuffer> autoCommitTransaction =
          new AutoCommitTransaction();

  private volatile State state = State.INIT;

  RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, TransactionManager transactionManager, LogManager logManager, ActionManager actionManager, Compactor compactor) {
    this.transactionManager = transactionManager;
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.compactor = compactor;
  }

  public RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, TransactionManager transactionManager, LogManager logManager, ActionManager actionManager) {
    this(objectManager, transactionManager, logManager, actionManager, new CompactorImpl(objectManager, transactionManager,
                                                                                         logManager,
                                                                                         new LSNGapCompactionPolicy(objectManager, logManager)));
  }

  @Override
  public synchronized Future<Void> startup() throws InterruptedException {
    if (state != State.INIT && state != State.SHUTDOWN) {
      throw new IllegalStateException("Can't startup from state " + state);
    }
    state = State.RECOVERING;
    RecoveryManager recoveryManager = new RecoveryManagerImpl(logManager, actionManager);
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
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginTransaction() {
    checkReadyState();
    return new TransactionImpl();
  }

  @Override
  public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> beginAutoCommitTransaction() {
    checkReadyState();
    return autoCommitTransaction;
  }

  private void checkReadyState() {
    if (state != State.RUNNING && state != State.RECOVERING) {
      throw new IllegalStateException("RestartStore is not ready for mutations.");
    }
  }

  private boolean isRecoverying() {
    return state == State.RECOVERING;
  }

  private class AutoCommitTransaction implements
          Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) throws
            TransactionException, InterruptedException {
      checkReadyState();
      transactionManager.happened(new PutAction(objectManager, compactor, id, key, value, isRecoverying()));
      return this;
  }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) throws
            TransactionException, InterruptedException {
      checkReadyState();
      transactionManager.happened(new DeleteAction(objectManager, compactor, id, isRecoverying()));
      return this;
    }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) throws
            TransactionException, InterruptedException {
      checkReadyState();
      transactionManager.happened(new RemoveAction(objectManager, compactor, id, key, isRecoverying()));
      return this;
    }

    @Override
    public void commit() throws InterruptedException, TransactionException {
    }
  }

  private class TransactionImpl implements
          Transaction<ByteBuffer, ByteBuffer, ByteBuffer> {
    private final TransactionHandle handle;
    private boolean committed = false;

    TransactionImpl() {
      this.handle = transactionManager.begin();
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new PutAction(objectManager, compactor, id, key, value, isRecoverying()));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new DeleteAction(objectManager, compactor, id, isRecoverying()));
      return this;
    }

    @Override
    public synchronized Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) {
      checkReadyState();
      checkCommitted();
      transactionManager.happened(handle, new RemoveAction(objectManager, compactor, id, key, isRecoverying()));
      return this;
    }

    @Override
    public synchronized void commit() throws InterruptedException, TransactionException {
      checkReadyState();
      checkCommitted();
      transactionManager.commit(handle);
      committed = true;
    }

    private void checkCommitted() {
      if (committed) throw new IllegalStateException("Transaction is already committed.");
    }
  }
}
