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
public class GCBufferSource implements BufferSource {

    public GCBufferSource() {
    }

    @Override
    public ByteBuffer getBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    @Override
    public void reclaim() {
        
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
        
    }
    
}
