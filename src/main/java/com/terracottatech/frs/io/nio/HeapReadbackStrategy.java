/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
public class HeapReadbackStrategy extends AbstractReadbackStrategy {
    private final FileBuffer                      buffer;
    private ListIterator<Chunk>   chunks;
    private Direction             queueDirection;
    
    public HeapReadbackStrategy(FileBuffer src, BufferSource buffers) throws IOException {
        buffer = src;
        queue(buffers, Direction.REVERSE);
    }
    
    @Override
    public Iterator<Chunk> iterator() {
        return chunks;
    }
    
    @Override
    public boolean hasMore(Direction dir) throws IOException {
        if ( dir == queueDirection && chunks.hasNext() ) return true;
        if ( dir != queueDirection && chunks.hasPrevious() ) return true;
        return false;
    }
    
 
    @Override
    public Chunk iterate(Direction dir) throws IOException {
        if ( dir == queueDirection && chunks.hasNext() ) return chunks.next();
        if ( dir != queueDirection && chunks.hasPrevious() ) return chunks.previous();
        return null;
    } 

    private void queue(BufferSource src, Direction dir) throws IOException {  
        queueDirection = dir;
//        ByteBuffer whole = src.getBuffer((int)(buffer.size()-NIOSegmentImpl.FILE_HEADER_SIZE));
        ByteBuffer whole = null;
        while ( whole == null ) {
            try {
                whole = ByteBuffer.allocate((int)(buffer.size()-NIOSegmentImpl.FILE_HEADER_SIZE));
//                whole = src.getBuffer((int)(buffer.size()-NIOSegmentImpl.FILE_HEADER_SIZE));
            } catch ( OutOfMemoryError out ) {
                System.gc();
            }
        }
        while (whole.hasRemaining()) {
            buffer.clear();
            if ( buffer.remaining() > whole.remaining() ) {
                buffer.partition(whole.remaining());
            }
            buffer.read(1);
            whole.put(buffer.getBuffers()[0]);
        }
        
        whole.flip();
        Chunk loaded = new WrappingChunk(whole);
        List<Chunk> list = new ArrayList<Chunk>();
        ByteBuffer[] chunk = readChunk(loaded);

        while (chunk != null) {
            list.add(new WrappingChunk(chunk));
            chunk = readChunk(loaded);
        }
        
        if ( dir == Direction.REVERSE ) Collections.reverse(list); 
        
        this.chunks = list.listIterator();
        this.queueDirection = dir;
    }
    
}
