/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.io.Serializable;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.messages.LogRecord;

/**
 *
 * @author cdennis
 */
class MockLogRecord implements LogRecord, Serializable {

  private final long previousLsn;
  private final long lowestLsn;
  private final Action action;
  
  private long lsn = -1;
  
  MockLogRecord(Action action, long lowestLsn) {
    //assert lsn > previousLsn;
    //assert previousLsn >= lowestLsn;

    this.action = action;
    this.previousLsn = action.getPreviousLsn();
    this.lowestLsn = lowestLsn;
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

  public void updateLsn(long lsn) {
    this.lsn = lsn;
    action.record(lsn);
  }

  Action getAction() {
    return action;
  }
}
