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
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.flash.ReadManager;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.recovery.RecoveryException;
import com.terracottatech.frs.recovery.RecoveryListener;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.recovery.RecoveryManagerImpl;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author twu
 */
public class RestartStoreImpl implements RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>,
        RecoveryListener {
  private enum State {
    INIT, RECOVERING, RUNNING, SHUTDOWN, PAUSED
  }

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager                                transactionManager;
  private final Compactor compactor;
  private final LogManager logManager;
  private final ActionManager actionManager;
  private final ReadManager readManager;
  private final Configuration configuration;

  private final int maxPauseTime;
  private final ExecutorService pauseTaskExecutorService;
  private final ScheduledExecutorService pauseTimeOutService;
  private volatile Future<Future<Snapshot>> pauseTaskRef;
  private volatile ScheduledFuture<?> pauseTimerTaskRef;

  private volatile State state = State.INIT;

  RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                   TransactionManager transactionManager, LogManager logManager,
                   ActionManager actionManager, ReadManager read, Compactor compactor,
                   Configuration configuration) {
    this.transactionManager = transactionManager;
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.actionManager = actionManager;
    this.readManager = read;
    this.compactor = compactor;
    this.configuration = configuration;
    this.pauseTaskExecutorService = Executors.newSingleThreadExecutor();
    this.pauseTimeOutService = Executors.newSingleThreadScheduledExecutor();
    this.maxPauseTime = configuration.getInt(FrsProperty.STORE_MAX_PAUSE_TIME_IN_MILLIS);
  }

  public RestartStoreImpl(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                          TransactionManager transactionManager, LogManager logManager,
                          ActionManager actionManager, ReadManager read, IOManager ioManager,
                          Configuration configuration) throws RestartStoreException {
    this(objectManager, transactionManager, logManager, actionManager, read, 
         new CompactorImpl(objectManager, transactionManager, logManager, ioManager, configuration,
                           actionManager),
         configuration);
  }

  @Override
  public synchronized Future<Void> startup() throws InterruptedException,
          RecoveryException {
    if (state != State.INIT) {
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

  @Override
  public Tuple<ByteBuffer, ByteBuffer, ByteBuffer> get(long marker) {
    try {
        LogRecord c = readManager.get(marker);
        if ( c == null ) {
            return null;
        }
        Action a = actionManager.extract(c);
        if ( a instanceof GettableAction ) {
          return (GettableAction)a;
        } else {
          throw new IllegalArgumentException("action is not a gettable event");
        }
    } catch ( InterruptedIOException ioe ) {
        Thread.currentThread().interrupt();
        return null;
    } catch ( IOException ioe ) {
        throw new RuntimeException("unrecoverable", ioe);
    }
  }

  @Override
  public synchronized Snapshot snapshot() throws RestartStoreException {
    checkReadyState();
    compactor.pause();
    try {
      return logManager.snapshot();
    } catch (Exception e) {
      throw new RestartStoreException(e);
    } finally {
      compactor.unpause();
    }
  }

  @Override
  public Statistics getStatistics() {
      return new Statistics() {
        private final IOStatistics delegate = logManager.getIOStatistics();
        @Override
        public long getTotalAvailable() {
          return delegate.getTotalAvailable();
        }

        @Override
        public long getTotalUsed() {
          return delegate.getTotalUsed();
        }

        @Override
        public long getTotalWritten() {
          return delegate.getTotalWritten();
        }

        @Override
        public long getTotalRead() {
          return delegate.getTotalRead();
        }

        @Override
        public long getLiveSize() {
          return delegate.getLiveSize();
        }

        @Override
        public long getExpiredSize() {
          return delegate.getExpiredSize();
        }
      };
  }

  @Override
  public synchronized Future<Future<Snapshot>> pause() {
    if (state == State.PAUSED) {
      return pauseTaskRef;
    }
    if (state != State.RUNNING) {
      throw new IllegalStateException("RestartStore is not ready for pause. Current state " + state);
    }
    state = State.PAUSED;
    pauseTimerTaskRef = pauseTimeOutService.schedule(new Runnable() {
      @Override
      public void run() {
        forceResume();
      }
    }, maxPauseTime, TimeUnit.MILLISECONDS);
    pauseTaskRef = pauseTaskExecutorService.submit(new Callable<Future<Snapshot>>() {
      @Override
      public Future<Snapshot> call() throws Exception {
        compactor.pause();
        actionManager.pause();
        return new OuterSnapshotFuture(logManager.snapshotAsync());
      }
    });
    return pauseTaskRef;
  }

  @Override
  public synchronized void resume() throws NotPausedException {
    if (state != State.PAUSED) {
      throw new NotPausedException("Restart store is currently not paused");
    }
    if (!pauseTaskRef.isDone()) {
      throw new IllegalStateException("Pause task is still running. This is unexpected");
    }
    actionManager.resume();
    pauseTimerTaskRef.cancel(true);
    pauseTaskRef = null;
    pauseTimerTaskRef = null;
    state = State.RUNNING;
  }

  /**
   * Force a resume as we have been in paused state for far too long and no one has
   * externally called a resume.
   */
  private synchronized void forceResume() {
    if (state != State.PAUSED) {
      return;
    }
    compactor.unpause();
    actionManager.resume();
    pauseTaskRef.cancel(true);
    pauseTaskRef = null;
    pauseTimerTaskRef = null;
    state = State.RUNNING;
  }

  private void checkReadyState() {
    if (state != State.RUNNING && state != State.RECOVERING && state != State.PAUSED) {
      throw new IllegalStateException("RestartStore is not ready for mutations. Current state " + state);
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

    private void happened(Action action) throws TransactionException {
      if (synchronous) {
        boolean interrupted = false;
        Future<Void> written = actionManager.syncHappened(action);
        while (true) {
          try {
            written.get();
            break;
          } catch (ExecutionException e) {
            throw new TransactionException(e);
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      } else {
        actionManager.happened(action);
      }

    }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> put(ByteBuffer id, ByteBuffer key, ByteBuffer value) throws
            TransactionException {
      checkReadyState();
      happened(new PutAction(objectManager, compactor, id, key, value, isRecovering()));
      return this;
  }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> delete(ByteBuffer id) throws
            TransactionException {
      checkReadyState();
      happened(new DeleteAction(objectManager, compactor, id, isRecovering()));
      return this;
    }

    @Override
    public Transaction<ByteBuffer, ByteBuffer, ByteBuffer> remove(ByteBuffer id, ByteBuffer key) throws
            TransactionException {
      checkReadyState();
      happened(new RemoveAction(objectManager, compactor, id, key, isRecovering()));
      return this;
    }

    @Override
    public void commit() throws TransactionException {
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
    public synchronized void commit() throws TransactionException {
      checkReadyState();
      checkCommitted();
      transactionManager.commit(handle, synchronous);
      committed = true;
    }

    private void checkCommitted() {
      if (committed) throw new IllegalStateException("Transaction is already committed.");
    }
  }

  /**
   * {@link Future} to wait for a snapshot to complete. The {@link OuterSnapshot} ensures that the
   * compactor is unpaused, once the inner snapshot is complete.
   */
  private class OuterSnapshotFuture implements Future<Snapshot> {
    private final Future<Snapshot> innerSnapshot;

    private OuterSnapshotFuture(Future<Snapshot> innerSnapshot) {
      this.innerSnapshot = innerSnapshot;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException("Not yet supported.");
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return innerSnapshot.isDone();
    }

    @Override
    public Snapshot get() throws InterruptedException, ExecutionException {
      Snapshot inner = innerSnapshot.get();
      return new OuterSnapshot(inner, compactor);
    }

    @Override
    public Snapshot get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      Snapshot inner = innerSnapshot.get(timeout, unit);
      return (inner != null) ? new OuterSnapshot(inner, compactor) : null;
    }
  }

  private static class OuterSnapshot implements Snapshot {
    private final Snapshot inner;
    private final Compactor compactor;

    public OuterSnapshot(Snapshot inner, Compactor compactor) {
      this.inner = inner;
      this.compactor = compactor;
    }

    @Override
    public void close() throws IOException {
      compactor.unpause();
      inner.close();
    }

    @Override
    public Iterator<File> iterator() {
      return inner.iterator();
    }
  }
}