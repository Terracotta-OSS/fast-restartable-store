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
public class CopyingChunk extends AbstractChunk {

   private final ByteBuffer[] list;
    
   public CopyingChunk(Chunk src) {
       ByteBuffer[] copy = src.getBuffers();
       list = new ByteBuffer[copy.length];
       int x=0;
       for ( ByteBuffer c : copy ) {
           list[x++] = (ByteBuffer)ByteBuffer.allocate(c.remaining()).put(c).flip();
       }
   }

    @Override
    public ByteBuffer[] getBuffers() {
        return list;
    } 
}
