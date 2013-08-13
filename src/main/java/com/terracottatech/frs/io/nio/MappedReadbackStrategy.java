/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
class MappedReadbackStrategy extends AbstractReadbackStrategy {

    private final   MappedByteBuffer        src;
    private final ListIterator<Chunk>       chunks;
    private final Direction                 queueDirection;
        
    public MappedReadbackStrategy(MappedByteBuffer data,Direction direction) throws IOException {
        src = data;
        queueDirection = direction;
        List<Long> jumps = readJumpList(src);
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
            Long last = Long.valueOf(NIOSegment.FILE_HEADER_SIZE);
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
            src.position(NIOSegment.FILE_HEADER_SIZE);
        }
        chunks = list.listIterator();
    }
    
    private boolean checkQueue(List<Chunk> got) throws IOException {
        List<Chunk> list = new ArrayList<Chunk>();
        src.clear().position(NIOSegment.FILE_HEADER_SIZE);
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
    public long size() {
        return src.capacity();
    }

    private int offset(long marker) {
        int move = 0;
        try {
            while ( this.hasMore(queueDirection) ) {
                Chunk c = iterate(queueDirection);
                long mark = c.getLong(c.length() - 12);
                if ( queueDirection == Direction.REVERSE && mark >= marker ) {
                    chunks.next();
                    return move;
                } else if ( queueDirection == Direction.FORWARD && mark <= marker ) {
                    chunks.previous();
                    return move;
                }
           }
        } catch ( IOException ioe ) {
            return -1;
        }
        return -1;
    }

    @Override
    public Chunk scan(long marker) throws IOException {
        if ( offset(marker) >= 0 ) {
            return iterate(queueDirection);
        }
        return null;
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
