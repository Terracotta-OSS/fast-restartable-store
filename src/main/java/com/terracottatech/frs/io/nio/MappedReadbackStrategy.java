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

    private final MappedByteBuffer                src;
    protected ListIterator<Chunk>   chunks;
    protected Direction             queueDirection;
    
    public MappedReadbackStrategy(FileChannel data) throws IOException {
        src = data.map(MapMode.READ_ONLY,0,(int)data.size());
        data.close();
        prepare(Direction.REVERSE);
    }
        
    public MappedReadbackStrategy(MappedByteBuffer data) throws IOException {
        src = data;
//        for(int y=data.position();y<data.limit();y=y+512) {
//            data.put(y, data.get(y));
//        }
        prepare(Direction.REVERSE);
    }
    
    private void prepare(Direction dir) throws IOException {
        queueDirection = dir;
        List<Long> jumps = readJumpList(new WrappingChunk(src));
        if ( jumps == null )  {
            queue(dir);
            return;
        }
        Long last = Long.valueOf(NIOSegmentImpl.FILE_HEADER_SIZE);
        ArrayList<Chunk> root = new ArrayList<Chunk>(jumps.size());
        for ( Long next : jumps ) {
            try {
                src.clear().position(last.intValue() + 12).limit(next.intValue() - 20);
                root.add(new MappedChunk(src,src.slice()));
            } catch ( Throwable t ) {
                throw new AssertionError(t);
            }
            last = next;
        }
        
        if ( dir == Direction.REVERSE ) Collections.reverse(root);

        chunks = root.listIterator();
        src.position(NIOSegmentImpl.FILE_HEADER_SIZE);
        
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
        
        return true;
    }
    
    public Chunk getBuffer() {
        return new WrappingChunk(src);
    }
    
    @Override
    public Iterator<Chunk> iterator() {
        return chunks;
    }
    
    @Override
    public boolean hasMore(Direction dir) throws IOException {
        if ( chunks == null ) queue(dir);
        if ( dir == queueDirection && chunks.hasNext() ) return true;
        if ( dir != queueDirection && chunks.hasPrevious() ) return true;
        return false;
    }

    public void queue(Direction dir) throws IOException {
        LinkedList<Chunk> list = new LinkedList<Chunk>();
        Chunk buf = getBuffer();
        ByteBuffer[] chunk = readChunk(buf);

        while (chunk != null) {
            if ( dir == Direction.REVERSE ) list.push(new MappedChunk(src, chunk));
            else list.add(new MappedChunk(src, chunk));
            chunk = readChunk(buf);
        }
        
        this.chunks = list.listIterator();
        this.queueDirection = dir;
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
        if ( chunks == null ) queue(dir);
        if ( dir == queueDirection && chunks.hasNext() ) return chunks.next();
        if ( dir != queueDirection && chunks.hasPrevious() ) return chunks.previous();
        return null;
    }
    
}
