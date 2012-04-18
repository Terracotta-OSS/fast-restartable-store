/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.action;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class MockLogRecord implements LogRecord, Serializable {

  private long lowestLsn;
  private final Action action;
  
  private long lsn = -1;
  
  MockLogRecord(Action action, long lowestLsn) {
    this.action = action;
    this.lowestLsn = lowestLsn;
  }

  public long getLsn() {
    return lsn;
  }

  public long getLowestLsn() {
    return lowestLsn;
  }
  
  public void setLowestLsn(long lsn) {
      lowestLsn = lsn;
  }

  public String toString() {
    String actionOut = action.toString();
    actionOut = "\t" + actionOut.replace("\n", "\n\t");
    
    return "LogRecord[lowest-lsn=" + getLowestLsn() + ", lsn=" + getLsn() + " {\n"
            + actionOut + "\n"
            + "}";
  }

  public void updateLsn(long lsn) {
    this.lsn = lsn;
    action.record(lsn);
  }

  public Action getAction() {
    return action;
  }

    @Override
    public ByteBuffer[] getPayload() {
        
        return new ByteBuffer[0];
    }
  
  
}
