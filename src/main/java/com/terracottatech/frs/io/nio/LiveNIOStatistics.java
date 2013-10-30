/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.IOStatistics;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author mscott
 */
class LiveNIOStatistics implements IOStatistics {
  
  private final File home;
  private final NIOStreamImpl stream;
  private final long written;
  private final long read;
  

  LiveNIOStatistics(File home, NIOStreamImpl stream, long written, long read) {
    this.home = home;
    this.stream = stream;
    this.written = written;
    this.read = read;
  }

  @Override
  public long getTotalAvailable() {
    return home.getUsableSpace();
  }

  @Override
  public long getTotalUsed() {
    return this.stream.getTotalSize();
  }

  @Override
  public long getTotalWritten() {
    return written;
  }

  @Override
  public long getTotalRead() {
    return read;
  }

  @Override
  public long getLiveSize() {
    try {
      return this.stream.findLogTail();
    } catch ( IOException ioe ) {
      return 0;
    }
  }

  @Override
  public long getExpiredSize() {
    try {
      return this.stream.getTotalSize() - this.stream.findLogTail();
    } catch ( IOException ioe ) {
      return 0;
    }
  }
  
}
