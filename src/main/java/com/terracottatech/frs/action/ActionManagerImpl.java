/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
  private enum State {
    NORMAL, WAITING_TO_PAUSE, PAUSED
  }

  private final LogManager             logManager;
  private final ObjectManager<?, ?, ?> objectManager;
  private final ActionCodec            actionCodec;
  private final LogRecordFactory       logRecordFactory;

  private final AtomicInteger          happeningCount;
  private volatile State               happenState;
  private final ReentrantLock          stateLock;
  private final Condition              happenedCondition;
  private final Condition              resumeCondition;

  public ActionManagerImpl(LogManager logManager, ObjectManager<?, ?, ?> objectManager,
                           ActionCodec actionCodec, LogRecordFactory logRecordFactory) {
    this.logManager = logManager;
    this.objectManager = objectManager;
    this.actionCodec = actionCodec;
    this.logRecordFactory = logRecordFactory;
    this.happeningCount = new AtomicInteger(0);
    this.happenState = State.NORMAL;
    this.stateLock = new ReentrantLock();
    this.happenedCondition = this.stateLock.newCondition();
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
  public void pause() {
    stateLock.lock();
    try {
      if (happenState != State.NORMAL) {
        return;
      }
      happenState = State.WAITING_TO_PAUSE;
      // once we are out of normal state.. other thread entering happened at the same moment will
      // get paused. If other thread has raced and won, the happening count will be non-zero and this
      // thread will hold until the happened() thread completes.
      if (happeningCount.get() == 0) {
        happenState = State.PAUSED;
      } else {
        boolean interrupted = false;
        while (happeningCount.get() != 0 && happenState == State.WAITING_TO_PAUSE) {
          try {
            this.happenedCondition.await();
          } catch (InterruptedException ie) {
            interrupted = true;
          }
        }
        if (happenState == State.WAITING_TO_PAUSE) {
          happenState = State.PAUSED;
        }
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public void resume() {
    stateLock.lock();
    try {
      if (happenState == State.NORMAL) {
        return;
      }
      happenState = State.NORMAL;
      this.happenedCondition.signal();
      this.resumeCondition.signalAll();
    } finally {
      stateLock.unlock();
    }
  }

  @Override
  public LogRecord barrierAction() {
    return wrapAction(new NullAction());
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