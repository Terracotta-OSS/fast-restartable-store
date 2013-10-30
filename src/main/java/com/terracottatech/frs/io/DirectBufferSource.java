/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class DirectBufferSource extends ManualBufferSource {

  public DirectBufferSource(long maxCapacity) {
    super(maxCapacity);
  }

  public DirectBufferSource(BufferSource parent, long maxCapacity) {
    super(parent, maxCapacity);
  }

    @Override
    protected ByteBuffer performAllocation(int size) {
        try {
            return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError err) {
            return null;
        }
    }
    
}
