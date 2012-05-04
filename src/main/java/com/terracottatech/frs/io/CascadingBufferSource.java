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
public class CascadingBufferSource implements BufferSource {
    private final BufferSource base;
    
    public CascadingBufferSource(BufferSource base) {
        this.base = base;
    }

    @Override
    public ByteBuffer getBuffer(int size) {
        ByteBuffer buffer = base.getBuffer(size);
        if ( buffer == null ) {
            buffer = ByteBuffer.allocate(size);
        }
        return buffer;
    }

    @Override
    public void returnByteBuffers(ByteBuffer[] buf) {
        base.returnByteBuffers(buf);
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
        base.returnBuffer(buffer);
    }

    @Override
    public void reclaim() {
        base.reclaim();
    }

    @Override
    public ByteBuffer[] getBuffers(long size) {
        return base.getBuffers(size);
    }
    
    
    
}
