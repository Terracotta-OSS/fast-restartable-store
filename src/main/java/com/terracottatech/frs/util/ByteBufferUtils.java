/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.util;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public abstract class ByteBufferUtils {
  public static final int LONG_SIZE = Long.SIZE / Byte.SIZE;
  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;
  public static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;
  public static final int BYTE_SIZE = Byte.SIZE / Byte.SIZE;
    
  private ByteBufferUtils() {}

  public static long getLong(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer.hasRemaining()) {
        return buffer.getLong();
      }
    }
    throw new BufferUnderflowException();
  }

  public static int getInt(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer.hasRemaining()) {
        return buffer.getInt();
      }
    }
    throw new BufferUnderflowException();
  }

  public static ByteBuffer getBytes(int length, ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer.hasRemaining()) {
        if (length > buffer.remaining()) {
          throw new UnsupportedOperationException("Length is spanning buffers...");
        }
        ByteBuffer buf = buffer.slice();
        buffer.position(length + buffer.position());
        buf.limit(length);
        return buf;
      }
    }
    throw new BufferUnderflowException();
  }

  public static ByteBuffer serializeLongSet(Set<Long> longs) {
    ByteBuffer buffer = ByteBuffer.allocate(
            LONG_SIZE * longs.size() + INT_SIZE);
    buffer.putInt(longs.size());
    for (long lsn : longs) {
      buffer.putLong(lsn);
    }
    buffer.flip();
    return buffer;
  }

  public static Set<Long> getLongSet(ByteBuffer[] buffers) {
    Set<Long> longs = new HashSet<Long>();
    int size = getInt(buffers);
    for (int i = 0; i < size; i++) {
      longs.add(getLong(buffers));
    }
    return longs;
  }
  
    
    public void flip(ByteBuffer[] list) {
        for (ByteBuffer buf : list ) {
            buf.flip();
        }
    }
    
    public void clear(ByteBuffer[] list) {
        for (ByteBuffer buf : list ) {
            buf.clear();
        }
    }
    
    public void skip(long jump, ByteBuffer[] list) {
        long count = 0;
        for (int x=0;x<list.length;x++) {
            if ( !list[x].hasRemaining() ) {
                continue;
            } else if ( jump - count > list[x].remaining() ) {
                count += list[x].remaining();
                list[x].position(list[x].limit());
            } else {
                list[x].position(list[x].position()+(int)(jump-count));
                return;
            }
        }
        throw new IndexOutOfBoundsException();
    }    
}
