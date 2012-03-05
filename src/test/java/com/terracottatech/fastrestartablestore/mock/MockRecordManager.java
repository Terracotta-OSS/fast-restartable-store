/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Compactor;
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
class MockRecordManager implements RecordManager {

  private final AtomicLong nextLsn = new AtomicLong();
  
  private final ObjectManager<?, ?, ?> objManager;
  private final LogManager logManager;
  private final Compactor compactor;
  
  public MockRecordManager(ObjectManager<?, ?, ?> objManager, LogManager logManager) {
    this.objManager = objManager;
    this.logManager = logManager;
    // This is ugly... is the Compactor really *part* of the RecordManager?
    this.compactor = new MockCompactor(this, objManager);
  }

  private long getNextLsn() {
    return nextLsn.getAndIncrement();
  }
  
  public synchronized Future<Void> happened(Action action) {
    long lsn = getNextLsn();
    long lowestLsn = objManager.getLowestLsn();
    long previousLsn = action.record(objManager, lsn);
    if (isValidId(previousLsn)) {
      compactor.compact(action);
    }
    LogRecord record = new MockLogRecord(lsn, previousLsn, lowestLsn, action);
    return logManager.append(record);
  }

  public synchronized void asyncHappened(Action action) {
    happened(action);
  }

  public Action extract(LogRecord record) throws IllegalArgumentException {
    if (record instanceof MockLogRecord) {
      return ((MockLogRecord) record).getAction();
    } else {
      throw new AssertionError();
    }
  }
  
  private boolean isValidId(long lsn) {
    return lsn >= 0;
  }
}
