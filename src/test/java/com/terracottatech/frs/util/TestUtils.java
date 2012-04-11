/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.util;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public abstract class TestUtils {
  private TestUtils() {}

  public static ByteBuffer byteBufferWithInt(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
    buffer.putInt(i).flip();
    return buffer;
  }
}
