/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRecord;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * @author cdennis
 */
class MockLogRegion implements LogRegion, Serializable {

  final LogRecord record;
  
  public MockLogRegion(LogRecord record) {
    this.record = record;
  }

    @Override
    public Iterator<LogRecord> iterator() {
        return Arrays.asList(record).iterator();
    }

  public String toString() {
    String recordOut = record.toString();
    recordOut = "\t" + recordOut.replace("\n", "\n\t");
    
    return "LogRegion[] {\n"
            + recordOut + "\n}";
  }
  
  private Object writeReplace() {
    return record;
  }
}
