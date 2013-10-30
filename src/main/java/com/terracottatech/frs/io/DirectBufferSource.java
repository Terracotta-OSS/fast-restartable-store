/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
