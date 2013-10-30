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
public class SimpleBufferSource implements BufferSource {

  @Override
  public ByteBuffer getBuffer(int size) {
    return ByteBuffer.allocate(size);
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {

  }

  @Override
  public void reclaim() {

  }
  
}
