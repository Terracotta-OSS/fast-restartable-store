/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.terracottatech.frs.util.NullFuture;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
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
  private static final Logger LOGGER = LoggerFactory.getLogger(RestartStoreImpl.class);

  private enum State {
    INIT, RECOVERING, RUNNING, SHUTDOWN, PAUSED, FROZEN
  }

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final TransactionManager                                transactionManager;
  private final Compactor compactor;
  private final LogManager logManager;
  private final ActionManager actionManager;
  private final ReadManager readManager;
  private final Configuration configuration;

  private final int maxPauseTime;
  private final ScheduledExecutorService pauseExecutionService;
  private volatile Future<Future<Snapshot>> pauseTaskRef;
  private volatile Future<Future<Void>> shutdownTaskRef;
  private volatile ScheduledFuture<?> pauseTimerTaskRef;

  private volatile State state = State.INIT;
  private volatile State prevState = state;

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
    this.pauseExecutionService = Executors.newScheduledThreadPool(0);
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
    while (state != State.INIT) {
      if (state == State.FROZEN) {
        // wait indefinitely as we cannot unfreeze from a frozen state
        // neither can we throw exception. Wait for JVM to die.
        LOGGER.warn("FRS Store is frozen. Waiting for a shutdown or resume");
        this.wait();
      } else {
        throw new IllegalStateException("Can't startup from state " + state);
      }
    }
    state = State.RECOVERING;
    RecoveryManager recoveryManager = new RecoveryManagerImpl(logManager, actionManager,
                                                              configuration);
    return recoveryManager.recover(this);
  }

  @Override
  public synchronized void recovered() throws InterruptedException {
    while (state == State.FROZEN) {
      LOGGER.warn("FRS Store is frozen. Waiting for a shutdown or resume");
      this.wait();
    }
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
      pauseExecutionService.shutdown();
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
    pauseTimerTaskRef = pauseExecutionService.schedule(new Runnable() {
      @Override
      public void run() {
        forceResume();
      }
    }, maxPauseTime, TimeUnit.MILLISECONDS);
    pauseTaskRef = pauseExecutionService.submit(new Callable<Future<Snapshot>>() {
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
    if (state != State.PAUSED && state != State.FROZEN) {
      throw new NotPausedException("Restart store is currently not paused");
    }
    if (state == State.PAUSED) {
      if (!pauseTaskRef.isDone()) {
        throw new IllegalStateException("Pause task is still running. This is unexpected");
      }
      pauseTimerTaskRef.cancel(true);
    }
    pauseTaskRef = null;
    pauseTimerTaskRef = null;
    shutdownTaskRef = null;
    if (state != State.FROZEN || prevState == State.RUNNING) {
      if (state == State.FROZEN) {
        // for snapshots..compactor is unpaused after snapshots are closed
        compactor.unpause();
      }
      actionManager.resume();
      state = State.RUNNING;
    } else {
      // state is frozen..means from Init or Recovered state..move back
      state = prevState;
      this.notifyAll();
    }
  }

  @Override
  public synchronized Future<Future<Void>> freeze() {
    if (shutdownTaskRef != null) {
      return shutdownTaskRef;
    }
    if (state == State.PAUSED) {
      // does not make sense to freeze when snapshot is going on
      throw new IllegalStateException("RestartStore is not ready for freeze. Snapshot may be in progress");
    }
    prevState = state;
    state = State.FROZEN;
    if (prevState != State.RUNNING) {
      // this means there is nothing to freeze.
      shutdownTaskRef = new NullFutureFuture();
      return shutdownTaskRef;
    } else {
      shutdownTaskRef = pauseExecutionService.submit(new Callable<Future<Void>>() {
        @Override
        public Future<Void> call() throws Exception {
          compactor.pause();
          actionManager.pause();
          return new OuterFreezeFuture(logManager.appendAndSync(actionManager.barrierAction()));
        }
      });
      return shutdownTaskRef;
    }
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
    if (isNotInReadyState(state)) {
      if (state != State.FROZEN || isNotInReadyState(prevState)) {
        throw new IllegalStateException("RestartStore is not ready for mutations. Current state " +
                                        (state == State.FROZEN ? prevState : state));
      }
    }
  }

  private boolean isNotInReadyState(State stateToCheck) {
    return (stateToCheck != State.RUNNING && stateToCheck != State.RECOVERING && stateToCheck != State.PAUSED);
  }

  private boolean isRecovering() {
    return state == State.RECOVERING || (state == State.FROZEN && prevState == State.RECOVERING);
  }

  private class NullFutureFuture implements Future<Future<Void>> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public Future<Void> get() throws InterruptedException, ExecutionException {
      return new NullFuture();
    }

    @Override
    public Future<Void> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return new NullFuture();
    }
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
   * Outer Freeze future that can be used to wait for a barrier action to reach durable storage.
   */
  private class OuterFreezeFuture implements Future<Void> {
    private final Future<Void> innerFreezeMarker;

    private OuterFreezeFuture(Future<Void> innerFreezeMarker) {
      this.innerFreezeMarker = innerFreezeMarker;
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
      return innerFreezeMarker.isDone();
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
      return innerFreezeMarker.get();
    }

    @Override
    public Void get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return innerFreezeMarker.get(timeout, unit);
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