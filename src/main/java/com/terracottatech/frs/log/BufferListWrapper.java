/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.AbstractChunk;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author mscott
 */
public class BufferListWrapper extends AbstractChunk {
    
    private final List<ByteBuffer> base;
    private ByteBuffer[]  converted;
    
    public BufferListWrapper(List<ByteBuffer> base) {
        this.base = base;
    }    

    @Override
    public ByteBuffer[] getBuffers() {
        if ( converted != null ) return converted;
        converted = base.toArray(new ByteBuffer[base.size()]);
        return converted;
    }
    
}
