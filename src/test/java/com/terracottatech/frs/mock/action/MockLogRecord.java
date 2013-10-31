/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.action;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import java.io.IOException;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class MockLogRecord implements LogRecord, Serializable {

  private final Action action;
  
  private long lsn = -1;
  
  MockLogRecord(Action action) {
    this.action = action;
  }

  public long getLsn() {
    return lsn;
  }

  public String toString() {
    String actionOut = action.toString();
    actionOut = "\t" + actionOut.replace("\n", "\n\t");
    
    return "LogRecord[lsn=" + getLsn() + " {\n"
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
  

  @Override
  public void close() throws IOException {

  }
}
