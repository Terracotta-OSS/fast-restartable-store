/*
 * All content copyright (c) 2014 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class LimitedCachingBufferSource extends CachingBufferSource {
  private final long limit;

  public LimitedCachingBufferSource(long limit) {
    this.limit = limit;
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    long size = super.getSize();
    while ( size - buffer.capacity() > limit && super.count() > 0 ) {
      size -= super.removeSmallest();
    }
    super.returnBuffer(buffer);
  }

  @Override
  public ByteBuffer getBuffer(int size) {
    if ( size > limit ) {
      return null;
    }
    return super.getBuffer(size); 
  }
  
  public long getLimit() {
    return limit;
  }
  
  
  
}
