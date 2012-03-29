/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.action;

import java.util.concurrent.Future;

import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.action.RecordManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.object.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockActionManager implements RecordManager {

  private final ObjectManager<?, ?, ?> objManager;
  private final LogManager logManager;
  
  public MockActionManager(ObjectManager<?, ?, ?> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
  }

  public Future<Void> happened(Action action) {
    LogRecord record = new MockLogRecord(action, objManager.getLowestLsn());
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
}
