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
    
    List<ByteBuffer> base;
    
    public BufferListWrapper(List<ByteBuffer> base) {
        this.base = base;
    }    

    @Override
    public ByteBuffer[] getBuffers() {
        return base.toArray(new ByteBuffer[base.size()]);
    }
    
}
