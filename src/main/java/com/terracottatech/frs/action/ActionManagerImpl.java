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
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRecordFactory;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author tim
 */
public class ActionManagerImpl implements ActionManager {

  private final LogManager             logManager;
  private final ObjectManager<?, ?, ?> objectManager;
  private final ActionCodec<?,?,?> actionCodec;
  private final LogRecordFactory       logRecordFactory;

  private final AtomicInteger          happeningCount;
  private final ReentrantLock          stateLock;
  private final Condition              idleCondition;
  private final Condition              resumeCondition;
  private volatile int pauseRequestCount = 0;

  public ActionManagerImpl(LogManager logManager, ObjectManager<?, ?, ?> objectManager,
                           ActionCodec<?,?,?> actionCodec, LogRecordFactory logRecordFactory) {
    this.logManager = logManager;
    this.objectManager = objectManager;
    this.actionCodec = actionCodec;
    this.logRecordFactory = logRecordFactory;
    this.happeningCount = new AtomicInteger(0);
    this.stateLock = new ReentrantLock();
    this.idleCondition = this.stateLock.newCondition();
    this.resumeCondition = this.stateLock.newCondition();
  }

  private LogRecord wrapAction(Action action) {
    ByteBuffer[] payload = actionCodec.encode(action);
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
  public Action extract(LogRecord record) {
    Action a = actionCodec.decode(record.getPayload());
    if ( a instanceof DisposableLifecycle ) {
        ((DisposableLifecycle)a).setDisposable(record);
    }
    return a;
  }

  @Override
  public Future<Void> syncHappenedAndPause(Action action) throws InterruptedException {
    stateLock.lock();
    try {
      pause();
      return logManager.appendAndSync(wrapAction(action));
    } finally {
      stateLock.unlock();
    }
  }
  
  @Override
  public void pause() throws InterruptedException {
    stateLock.lock();
    try {
      pauseRequestCount++;
      while(happeningCount.get() > 0) {
        idleCondition.await();
      }
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public void resume() {
    stateLock.lock();
    try {
      if (--pauseRequestCount == 0) {
        resumeCondition.signalAll();
      }
    } finally {
      stateLock.unlock();
    }
  }

  /**
   * Checks if gate is closed before executing the action manager 'happened' call.
   * Uses an optimistic approach to avoid holding the stateLock during normal operations.
   * <p>
   * Optimistically increment and do an unprotected check with a volatile read to avoid holding locks as action manager
   * pause is rare operation done during backup and rotating enc key.
   */
  private void enterHappened() {
    happeningCount.incrementAndGet();
    if (pauseRequestCount > 0) {
      stateLock.lock();
      try {
        if (pauseRequestCount > 0) {
          try {
            if (happeningCount.decrementAndGet() == 0) {
              this.idleCondition.signalAll();
            }
            while (pauseRequestCount > 0) {
              resumeCondition.awaitUninterruptibly();
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
    int remaining = happeningCount.decrementAndGet();
    if (pauseRequestCount > 0) {
      stateLock.lock();
      try {
        if (remaining == 0) {
          idleCondition.signalAll();
        }
      } finally {
        stateLock.unlock();
      }
    }
  }
}