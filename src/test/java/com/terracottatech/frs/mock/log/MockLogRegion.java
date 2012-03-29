/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
