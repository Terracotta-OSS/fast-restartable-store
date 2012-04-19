/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 *
 * @author mscott
 */
public class MappedFileBuffer extends AbstractChunk implements Closeable {
    
    private final java.nio.MappedByteBuffer base;
    private       ByteBuffer[]              buffer;

    public MappedFileBuffer(java.nio.MappedByteBuffer src) {
        this.base = src;
        this.buffer = new ByteBuffer[] {src};
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return buffer;
    }

    public MappedFileBuffer partition(int... pos) {
        ByteBuffer target = base.duplicate();
        
        ArrayList<ByteBuffer> sections = new ArrayList<ByteBuffer>();
        for (int p : pos) {
            if ( p > target.limit() ) {
                throw new BufferUnderflowException();
            } else {
                sections.add((ByteBuffer)target.slice().limit(p-target.position()));
                target.position(target.position() + p);
            }
        }
        sections.add((ByteBuffer)target.slice());
        
        buffer = sections.toArray(new ByteBuffer[sections.size()]);
        return this;
    }

    @Override
    public void close() throws IOException {
        base.force();
    }
    
    
    
}
