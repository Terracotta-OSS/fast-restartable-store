/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;
import java.io.Serializable;

/**
 *
 * @author cdennis
 */
class MockLogRecord implements LogRecord, Serializable {

  private final long lsn;
  private final long previousLsn;
  private final long lowestLsn;
  private final Action action;
  
  MockLogRecord(long lsn, long previousLsn, long lowestLsn, Action action) {
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
    String actionOut = action.toString();
    actionOut = "\t" + actionOut.replace("\n", "\n\t");
    
    return "LogRecord[lowest-lsn=" + getLowestLsn() + ", previous-lsn=" + getPreviousLsn() + ", lsn=" + getLsn() + " {\n"
            + actionOut + "\n"
            + "}";
  }

  Action getAction() {
    return action;
  }
}
