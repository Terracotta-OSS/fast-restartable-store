/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.action;

import java.io.Serializable;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class MockLogRecord implements LogRecord, Serializable {

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

  public Action getAction() {
    return action;
  }

    @Override
    public ByteBuffer[] getPayload() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oo = new ObjectOutputStream(out);
            oo.writeObject(this);
            oo.close();
        } catch ( IOException ioe ) {
            throw new AssertionError(ioe);
        }
        
        return new ByteBuffer[] {ByteBuffer.wrap(out.toByteArray())};
    }
  
  
}
