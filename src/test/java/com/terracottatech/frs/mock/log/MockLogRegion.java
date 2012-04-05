/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRecord;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
class MockLogRegion implements LogRegion, Chunk, Serializable {

  final LogRecord record;
  
  public MockLogRegion(LogRecord record) {
    this.record = record;
  }

    @Override
    public ByteBuffer[] getBuffers() {
        return record.getPayload();
    }

    @Override
    public long length() {
        long len = 0;
        for ( ByteBuffer buf : record.getPayload() ) {
            len += buf.remaining();
        }
        return len;
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
