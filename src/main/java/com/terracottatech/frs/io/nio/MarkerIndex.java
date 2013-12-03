/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.SimpleBufferSource;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 *
 * @author mscott
 */
public class MarkerIndex implements Closeable {
  private final BufferSource source;
  private ByteBuffer  jumpIndex;
  private volatile int cacheCount = 0;

  public MarkerIndex(BufferSource source) {
    if ( source == null ) {
      source = new SimpleBufferSource();
    }
    this.source = source;
  }
  
  public long position(int index) {
    return jumpIndex.getLong(ByteBufferUtils.LONG_SIZE * (2 * index));
  }
    
  public long mark(int index) {
 //  volatle read as a memory barrier
    if ( cacheCount > 0 ) {
      return jumpIndex.getLong(ByteBufferUtils.LONG_SIZE * (2 * index + 1));
    }
    return Long.MIN_VALUE;
  }
  
  public int size() {
    if ( jumpIndex == null ) {
      return 0;
    }
    return jumpIndex.position() / (ByteBufferUtils.LONG_SIZE * 2);
  }
  
  public void cache(int index, long mark) {
    cacheCount++;
    jumpIndex.putLong(ByteBufferUtils.LONG_SIZE * (2 * index + 1),mark);
  }
  
  public final void append(long[] adding) {
    expand(adding.length);
    for ( Long pos : adding) {
      jumpIndex.putLong(pos);
      jumpIndex.putLong(0L);
    }
  }
  
  public final void append(Collection<Long> adding) {
    expand(adding.size());
    for ( Long pos : adding) {
      jumpIndex.putLong(pos);
      jumpIndex.putLong(0L);
    }
  }  
  
  private void expand(int size) {
    if ( jumpIndex == null || jumpIndex.remaining() < ByteBufferUtils.LONG_SIZE * 2 * (size) ) {
      int cap = ByteBufferUtils.LONG_SIZE * 2 * (size);
      if ( jumpIndex != null ) {
        cap += jumpIndex.position();
      }
      ByteBuffer newspace = null;
      if ( cap < 256 * 1024 ) {
        newspace = source.getBuffer(cap);
      } else {
        if ( cap < 1024 * 1024 ) {
          cap = 1024 * 1024;
        }
        newspace = ByteBuffer.allocateDirect(cap);
      }
      if ( jumpIndex != null ) {
        jumpIndex.flip();
        newspace.put(jumpIndex);
        source.returnBuffer(jumpIndex);
      }
      jumpIndex = newspace;
    }
  }

  @Override
  public void close() throws IOException {
    source.returnBuffer(jumpIndex);
  }

  @Override
  public String toString() {
    return "MarkerIndex{" + "source=" + jumpIndex + ", cacheCount=" + cacheCount + '}';
  }
  
  
}
