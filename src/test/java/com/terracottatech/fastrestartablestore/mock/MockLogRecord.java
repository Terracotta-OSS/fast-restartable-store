/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;

/**
 *
 * @author cdennis
 */
class MockLogRecord implements LogRecord {

  private final long lsn;
  private final long previousLsn;
  private final long lowestLsn;
  private final Action<?> action;
  
  MockLogRecord(long lsn, long previousLsn, long lowestLsn, Action<String> action) {
    //assert lsn > previousLsn;
    //assert previousLsn >= lowestLsn;
    
    this.lsn = lsn;
    this.previousLsn = previousLsn;
    this.lowestLsn = lowestLsn;
    this.action = action;
  }

  public long getLsn() {
    return lsn;
  }

  public long getPreviousLsn() {
    return previousLsn;
  }

  public long getLowestLsn() {
    return lowestLsn;
  }

  public String toString() {
    return "LogRecord[\n"
            + "\tlowest-lsn=" + getLowestLsn() + "\n"
            + "\tprevious-lsn=" + getPreviousLsn() + "\n"
            + "\tlsn=" + getLsn() + "\n"
            + "\t{\n"
            + action + "\n"
            + "\t}";
  }
}
