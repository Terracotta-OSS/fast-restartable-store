/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class MaskingBufferSource implements BufferSource {
  
  private final BufferSource parent;
  private final Map<BufferEquals, ByteBuffer> out = new ConcurrentHashMap<BufferEquals, ByteBuffer>();
  private static final Logger LOGGER = LoggerFactory.getLogger(MaskingBufferSource.class);

  public MaskingBufferSource(BufferSource parent) {
    this.parent = parent;
  }

  @Override
  public ByteBuffer getBuffer(int size) {
    ByteBuffer src = parent.getBuffer(size);
    if ( src == null ) {
      return ByteBuffer.allocate(size);
    }
    return add(src);
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    ByteBuffer src = remove(new BufferEquals(buffer));
    if ( src != null ) {
      parent.returnBuffer(src);
    } else {
      LOGGER.debug("possible double free of memory resource" + buffer);
    }
  }
  
  private ByteBuffer remove(BufferEquals src) {
    return out.remove(src);
  }
  
  private ByteBuffer add(ByteBuffer src) {
    ByteBuffer pass = src.slice();
    out.put(new BufferEquals(pass), src);
    return pass;
  }

  @Override
  public void reclaim() {
    for ( Map.Entry<BufferEquals,ByteBuffer> b : new ArrayList<Map.Entry<BufferEquals,ByteBuffer>>(out.entrySet()) ) {
      parent.returnBuffer(b.getValue());
    }
    out.clear();
    parent.reclaim();
  }
  
  
  private static class BufferEquals {
    
    private final ByteBuffer src;
    
    public BufferEquals(ByteBuffer src) {
      this.src = src;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(src);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final BufferEquals other = (BufferEquals) obj;
      return (this.src == other.src);
    }
  }

  @Override
  public String toString() {
    return "MaskingBufferSource{" + "parent=" + parent + ", out=" + out.size() + '}';
  }
  
  
}
