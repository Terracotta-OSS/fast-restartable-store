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
public class WrappingChunk extends AbstractChunk {
    
    ByteBuffer[] base;

    public WrappingChunk(ByteBuffer base) {
        this.base = new ByteBuffer[] {base};
    }
    
    public WrappingChunk(ByteBuffer[] bufs) {
        this.base = bufs;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return base;
    }
    
    
}
