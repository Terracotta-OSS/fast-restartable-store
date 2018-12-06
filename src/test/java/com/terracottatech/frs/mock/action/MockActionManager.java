/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.action;

import java.util.concurrent.Future;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.object.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockActionManager implements ActionManager {

  private final ObjectManager<?, ?, ?> objManager;
  private final LogManager logManager;
  
  public MockActionManager(ObjectManager<?, ?, ?> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
  }

  public Future<Void> happened(Action action) {
    LogRecord record = new MockLogRecord(action);
    return logManager.append(record);
  }

  public void asyncHappened(Action action) {
    happened(action);
  }

  public Action extract(LogRecord record) throws IllegalArgumentException {
    if (record instanceof MockLogRecord) {
      Action action = ((MockLogRecord) record).getAction();
      if (action instanceof MockAction) {
        ((MockAction) action).setObjectManager(objManager);
      }
      return action;
    } else {
      throw new AssertionError();
    }
  }

  @Override
  public void pause() {
  }

  @Override
  public void resume() {
  }

  @Override
  public LogRecord barrierAction() {
    return null;
  }

  @Override
  public Future<Void> syncHappened(Action action) {
    return happened(action);
  }
}
