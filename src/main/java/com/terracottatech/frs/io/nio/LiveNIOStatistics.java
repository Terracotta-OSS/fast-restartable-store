/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
      return this.stream.getTotalSize() - this.stream.scanForEnd();
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

  @Override
  public String toString() {
    return "LiveNIOStatistics{" + "home=" + home + ", written=" + written + ", read=" + read + 
        " used:" + getTotalUsed() + 
        '}';
  }
}
