/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author mscott
 */
public class AppendableChunk extends AbstractChunk {
    private ByteBuffer[] buffers;

    public AppendableChunk(List<ByteBuffer>  base) {
        this.buffers = base.toArray(new ByteBuffer[base.size()]);
    }
 
    public AppendableChunk(ByteBuffer[]  base) {
        this.buffers = base;
    }   
    
    public AppendableChunk copy() {
        ByteBuffer[] cb = Arrays.copyOf(buffers, buffers.length);
        for ( int x=0;x<cb.length;x++ ) {
            cb[x] = cb[x].duplicate();
        }
        return new AppendableChunk(cb);
    }
    
    public void append(ByteBuffer add) {
        ByteBuffer[] list = Arrays.copyOf(buffers, buffers.length + 1);
        list[buffers.length] = add;
        buffers = list;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return buffers;
    }

}
