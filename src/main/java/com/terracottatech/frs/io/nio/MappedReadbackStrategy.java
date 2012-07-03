/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

/**
 *
 * @author mscott
 */
class MappedReadbackStrategy extends AbstractReadbackStrategy {

    private final   MappedByteBuffer    src;
    private final ListIterator<Chunk>       chunks;
    private final Direction             queueDirection;
        
    public MappedReadbackStrategy(MappedByteBuffer data,Direction direction) throws IOException {
        src = data;
        queueDirection = direction;
        List<Long> jumps = readJumpList(new WrappingChunk(src));
        LinkedList<Chunk> list = new LinkedList<Chunk>();
        if ( jumps == null )  {
            Chunk buf = new WrappingChunk(data);
            ByteBuffer[] chunk = readChunk(buf);

            while (chunk != null) {
                if ( queueDirection == Direction.REVERSE ) list.push(new WrappingChunk(chunk));
                else list.add(new WrappingChunk(chunk));
                chunk = readChunk(buf);
            }
            
        } else {
            Long last = Long.valueOf(NIOSegmentImpl.FILE_HEADER_SIZE);
            for ( Long next : jumps ) {
                try {
                    src.clear().position(last.intValue() + 12).limit(next.intValue() - 20);
                    if ( queueDirection == Direction.REVERSE ) list.push(new WrappingChunk(src.slice()));
                    else list.add(new WrappingChunk(src.slice()));
                } catch ( Throwable t ) {
                    throw new AssertionError(t);
                }
                last = next;
            }
            src.position(NIOSegmentImpl.FILE_HEADER_SIZE);
        }
        chunks = list.listIterator();
    }
    
    private boolean checkQueue(List<Chunk> got) throws IOException {
        List<Chunk> list = new ArrayList<Chunk>();
        src.clear().position(NIOSegmentImpl.FILE_HEADER_SIZE);
        Chunk buf = getBuffer();
        ByteBuffer[] chunk = readChunk(buf);

        while (chunk != null) {
            list.add(new WrappingChunk(chunk));
            chunk = readChunk(buf);
        }
        
        if ( got != null ) {
            if ( got.size() != list.size() ) {
                System.out.println(got.size() + " " + list.size());
                return false;
            }
            for (int x=0;x<got.size();x++) {
                if ( got.get(x).remaining() != list.get(x).remaining()  ) {
                    System.out.println(x + " " + got.get(x).remaining() + " " + list.get(x).remaining());
                    return false;
                }
            }
        }
        
        return super.isConsistent();
    }

    @Override
    public boolean isConsistent() {
        try {
            return checkQueue(null);
        } catch ( IOException ioe ) {
            return false;
        }
    }

    public Chunk getBuffer() {
        return new WrappingChunk(src);
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
    
}
