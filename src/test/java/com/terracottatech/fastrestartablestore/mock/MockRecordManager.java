/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author cdennis
 */
class MockRecordManager implements RecordManager<String> {

  private final AtomicLong nextLsn = new AtomicLong();
  
  private final ObjectManager<String, String> objManager;
  private final LogManager logManager;
  
  public MockRecordManager(ObjectManager<String, String> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
  }

  private long getNextLsn() {
    return nextLsn.getAndIncrement();
  }
  
  public synchronized Future<Void> happened(Action<String> action) {
    long lsn = getNextLsn();
    long previousLsn = -1;
    long lowestLsn = objManager.getLowestLsn();
    if (action.hasKey()) {
      previousLsn = objManager.updateLsn(action.getKey(), lsn);
    }
    LogRecord record = new MockLogRecord(lsn, previousLsn, lowestLsn, action);
    return logManager.append(record);
  }

  public synchronized void asyncHappened(Action<String> action) {
    happened(action);
  }

  public Action extract(LogRecord record) throws IllegalArgumentException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
