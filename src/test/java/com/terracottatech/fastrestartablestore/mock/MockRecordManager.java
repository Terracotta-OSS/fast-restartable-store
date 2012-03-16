/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.concurrent.Future;

import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
class MockRecordManager implements RecordManager {

  private final ObjectManager<?, ?, ?> objManager;
  private final LogManager logManager;
  
  public MockRecordManager(ObjectManager<?, ?, ?> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
  }

  public synchronized Future<Void> happened(Action action) {
    LogRecord record = new MockLogRecord(action, objManager.getLowestLsn());
    return logManager.append(record);
  }

  public synchronized void asyncHappened(Action action) {
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
