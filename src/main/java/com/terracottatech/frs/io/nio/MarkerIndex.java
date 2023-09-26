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

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.SimpleBufferSource;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class MarkerIndex implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarkerIndex.class);
  private final BufferSource source;
  private LongBuffer  jumpIndex;
  private ByteBuffer  returnBuffer;
  private volatile int cacheCount = 0;

  public MarkerIndex(BufferSource source) {
    if ( source == null ) {
      source = new SimpleBufferSource();
    }
    this.source = source;
  }
  
  public long position(int index) {
    if (jumpIndex == null) {
      return 0L;
    }
    return jumpIndex.get(2 * index);
  }
    
  public long mark(int index) {
 //  volatle read as a memory barrier
    if ( cacheCount > 0 ) {
      return jumpIndex.get(2 * index + 1);
    }
    return Long.MIN_VALUE;
  }
  
  public int size() {
    if ( jumpIndex == null ) {
      return 0;
    }
    return jumpIndex.position() / 2;
  }
  
  public void cache(int index, long mark) {
    cacheCount++;
    jumpIndex.put(2 * index + 1,mark);
  }
  
  public final void append(long[] adding) {
    expand(adding.length);
    for ( Long pos : adding) {
      jumpIndex.put(pos);
      jumpIndex.put(0L);
    }
  }
  
  public final void append(Collection<Long> adding) {
    expand(adding.size());
    for ( Long pos : adding) {
      jumpIndex.put(pos);
      jumpIndex.put(0L);
    }
  }  
  
  private void expand(int size) {
    ByteBuffer oldReturn = returnBuffer;
    if ( jumpIndex == null || jumpIndex.remaining() < 2 * size ) {
      int cap = 2 * (size);
      if ( jumpIndex != null ) {
        cap += jumpIndex.position();
      }
      LongBuffer newspace;
    LOGGER.debug("expanding index to " + (cap  * ByteBufferUtils.LONG_SIZE));
      if ( cap < 2048 / ByteBufferUtils.LONG_SIZE) {
        returnBuffer = source.getBuffer(cap  * ByteBufferUtils.LONG_SIZE);
        newspace = returnBuffer.asLongBuffer();
      } else {
        if ( cap < 1024 * 1024 / ByteBufferUtils.LONG_SIZE) {
          cap = 1024 * 1024 / ByteBufferUtils.LONG_SIZE;
        }
    LOGGER.debug("not using buffer source");
        try {
          newspace = ByteBuffer.allocateDirect(cap * ByteBufferUtils.LONG_SIZE).asLongBuffer();
        } catch ( OutOfMemoryError oome ) {
//  assume offheap
          newspace = ByteBuffer.allocate(cap * ByteBufferUtils.LONG_SIZE).asLongBuffer();
        }
        returnBuffer = null;
      }
      if ( jumpIndex != null ) {
        jumpIndex.flip();
        newspace.put(jumpIndex);
      }
      jumpIndex = newspace;
      if ( oldReturn != null ) {
        source.returnBuffer(oldReturn);
      }
    }
  }

  @Override
  public void close() throws IOException {
    source.returnBuffer(returnBuffer);
  }

  @Override
  public String toString() {
    return "MarkerIndex{" + "source=" + jumpIndex + ", cacheCount=" + cacheCount + '}';
  }
  
  
}
