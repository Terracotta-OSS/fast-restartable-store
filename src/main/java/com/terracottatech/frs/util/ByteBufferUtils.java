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
}
