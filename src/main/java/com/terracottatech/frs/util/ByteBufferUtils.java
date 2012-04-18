/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.util;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * @author tim
 */
public abstract class ByteBufferUtils {
  public static int SHORT_SIZE = Short.SIZE / Byte.SIZE;
  public static int LONG_SIZE = Long.SIZE / Byte.SIZE;
  public static int INT_SIZE = Integer.SIZE / Byte.SIZE;

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
