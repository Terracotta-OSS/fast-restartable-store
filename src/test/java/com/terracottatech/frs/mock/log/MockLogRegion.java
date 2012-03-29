/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.io.LogRegion;
import com.terracottatech.frs.log.LogRecord;
import java.io.Serializable;

/**
 *
 * @author cdennis
 */
class MockLogRegion implements LogRegion, Serializable {

  final LogRecord record;
  
  public MockLogRegion(LogRecord record) {
    this.record = record;
  }

  public long getLowestLsn() {
    return record.getLowestLsn();
  }

  public String toString() {
    String recordOut = record.toString();
    recordOut = "\t" + recordOut.replace("\n", "\n\t");
    
    return "LogRegion[lowest-lsn=" + getLowestLsn() + "] {\n"
            + recordOut + "\n}";
  }
  
  private Object writeReplace() {
    return record;
  }
}
