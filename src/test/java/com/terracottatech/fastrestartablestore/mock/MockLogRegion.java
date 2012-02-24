/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Chunk;
import com.terracottatech.fastrestartablestore.messages.LogRecord;

/**
 *
 * @author cdennis
 */
class MockLogRegion implements Chunk {

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
}
