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
package com.terracottatech.frs.action;

import com.terracottatech.frs.DisposableLifecycle;
import com.terracottatech.frs.cipher.EncryptionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRecordFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionAccount;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionalAction;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tim
 */
public class ActionManagerImpl implements ActionManager {
  private enum State {
    NORMAL, WAITING_TO_PAUSE, PAUSED
  }

  private final LogManager             logManager;
  private final ObjectManager<?, ?, ?> objectManager;
  private final EncryptionManager encryptionManager;
  private final ActionCodec            actionCodec;
  private final LogRecordFactory       logRecordFactory;

  private final AtomicInteger          happeningCount;
  private volatile State               happenState;
  private final ReentrantLock          stateLock;
  private final Condition              happenedCondition;
  private final Condition              resumeCondition;

  public ActionManagerImpl(LogManager logManager, ObjectManager<?, ?, ?> objectManager,
                           EncryptionManager encryptionManager, ActionCodec actionCodec, LogRecordFactory logRecordFactory) {
    this.logManager = logManager;
    this.objectManager = objectManager;
    this.encryptionManager = encryptionManager;
    this.actionCodec = actionCodec;
    this.logRecordFactory = logRecordFactory;
    this.happeningCount = new AtomicInteger(0);
    this.happenState = State.NORMAL;
    this.stateLock = new ReentrantLock();
    this.happenedCondition = this.stateLock.newCondition();
    this.resumeCondition = this.stateLock.newCondition();
  }

  private LogRecord wrapAction(Action action) {
    ByteBuffer[] payload = actionCodec.encode(encryptionManager.convert(action));
    return logRecordFactory.createLogRecord(payload, action);
  }

  @Override
  public Future<Void> syncHappened(Action action) {
    enterHappened();
    try {
      return logManager.appendAndSync(wrapAction(action));
    } finally {
      // For action manage pause, we just have to track that the thread executing action
      // manager happened is out. We do not need to ensure that current queued IO is made
      // durable at this point.
      exitHappened();
    }
  }

  @Override
  public Future<Void> happened(Action action) {
    enterHappened();
    try {
      return logManager.append(wrapAction(action));
    } finally {
      exitHappened();
    }
  }

  @Override
  public Future<Void> happenedTransactionally(Action action, TransactionHandle handle, TransactionAccount account) {
    enterHappened();
    try {
      Action wrapped = encryptionManager.convert(action);
      Action transactionalAction = new TransactionalAction(handle, account.begin(), false, wrapped, account);
      LogRecord logRecord = logRecordFactory.createLogRecord(actionCodec.encode(transactionalAction), transactionalAction);
      return logManager.append(logRecord);
    } finally {
      exitHappened();
    }
  } 
  
  @Override
  public Action extract(LogRecord record) {
    Action a = actionCodec.decode(record.getPayload());
    if ( a instanceof DisposableLifecycle ) {
        ((DisposableLifecycle)a).setDisposable(record);
    }
    return a;
  }

  @Override
  public Future<Void> pause(Action action) throws InterruptedException {
    stateLock.lock();
    try {
      while (happenState != State.NORMAL) {
        happenedCondition.await();
      }
      happenState = State.WAITING_TO_PAUSE;
      // once we are out of normal state.. other thread entering happened at the same moment will
      // get paused. If other thread has raced and won, the happening count will be non-zero and this
      // thread will hold until the happened() thread completes.
      if (happeningCount.get() == 0) {
        happenState = State.PAUSED;
      } else {
        while (happeningCount.get() != 0 && happenState == State.WAITING_TO_PAUSE) {
            this.happenedCondition.await();
        }
        if (happenState == State.WAITING_TO_PAUSE) {
          happenState = State.PAUSED;
        }
      }
      return logManager.appendAndSync(wrapAction(action));
    } finally {
      stateLock.unlock();
    }
  }
  
  @Override
  public Future<Void> pause() throws InterruptedException {
    return pause(new NullAction());
  }

  @Override
  public void resume() {
    stateLock.lock();
    try {
      if (happenState == State.NORMAL) {
        return;
      }
      happenState = State.NORMAL;
      this.happenedCondition.signalAll();
      this.resumeCondition.signalAll();
    } finally {
      stateLock.unlock();
    }
  }
  
  /**
   * Checks if gate is closed before executing the action manager 'happened' call.
   * Uses an optimistic approach to avoid holding the stateLock during normal operations.
   * <p>
   * Optimistically increment and do an unprotected check with a volatile read to avoid holding locks as action manager
   * changing state from NORMAL to anything else is rare and happens only during backups.
   */
  private void enterHappened() {
    happeningCount.incrementAndGet();
    if (happenState != State.NORMAL) {
      stateLock.lock();
      try {
        if (happenState != State.NORMAL) {
          int happenedCnt = happeningCount.decrementAndGet();
          try {
            if (happenedCnt == 0) {
              this.happenedCondition.signal();
            }
            boolean interrupted = false;
            while (happenState != State.NORMAL) {
              try {
                resumeCondition.await();
              } catch (InterruptedException e) {
                interrupted = true;
              }
            }
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          } finally {
            happeningCount.incrementAndGet();
          }
        }
      } finally {
        stateLock.unlock();
      }
    }
  }

  private void exitHappened() {
    int numIn = happeningCount.decrementAndGet();
    // ok to do a dirty check first..
    if (happenState != State.NORMAL) {
      stateLock.lock();
      try {
        if (numIn == 0 && happenState == State.WAITING_TO_PAUSE) {
          this.happenedCondition.signal();
        }
      } finally {
        stateLock.unlock();
      }
    }
  }
}