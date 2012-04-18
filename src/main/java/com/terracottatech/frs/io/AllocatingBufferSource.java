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
public class AllocatingBufferSource implements BufferSource {
    
    private ByteBuffer soleSource;

    @Override
    public ByteBuffer getBuffer(int size) {
        if ( soleSource == null) {
            soleSource = ByteBuffer.allocateDirect(size);
            return soleSource;
        }
        if ( soleSource.limit() != 0 ) throw new AssertionError("source in use");
        if ( soleSource.capacity() < size ) soleSource = ByteBuffer.allocateDirect(size);
        return (ByteBuffer)soleSource.clear().limit(size);
    }

    @Override
    public ByteBuffer[] getBuffers(long size) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
   //  probably does nothing
        assert(buffer == soleSource);
        buffer.limit(0);
    }

    @Override
    public void returnByteBuffers(ByteBuffer[] buf) {

    }

    @Override
    public void reclaim() {
        if ( soleSource != null ) soleSource.limit(0);
        soleSource = null;
    }
    
    
    
}
