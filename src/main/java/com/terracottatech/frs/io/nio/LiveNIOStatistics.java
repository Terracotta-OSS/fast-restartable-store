/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
