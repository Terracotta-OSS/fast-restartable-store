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

//    MappedFileBuffer                      src;
    private final MappedByteBuffer                src;
    protected ListIterator<Chunk>   chunks;
    protected Direction             queueDirection;
    private final FileChannel                     channel;
    
    public MappedReadbackStrategy(FileChannel data) throws IOException {
//        src = new MappedFileBuffer(data,MapMode.READ_ONLY,(int)data.size());
        src = data.map(MapMode.READ_ONLY,0,(int)data.size());
        channel = data;
        data.close();
        prepare(Direction.REVERSE);
    }
        
    public MappedReadbackStrategy(MappedByteBuffer data) throws IOException {
//        src = new MappedFileBuffer(data,MapMode.READ_ONLY,(int)data.size());
        src = data;
        channel = null;
        prepare(Direction.REVERSE);
    }
    
    private void prepare(Direction dir) throws IOException {
        queueDirection = dir;
//        src.load();
        List<Long> jumps = readJumpList(new WrappingChunk(src));
        if ( jumps == null )  {
            queue(dir);
            return;
        }
        Long last = new Long(NIOSegmentImpl.FILE_HEADER_SIZE);
        ArrayList<Chunk> root = new ArrayList<Chunk>(jumps.size());
        for ( Long next : jumps ) {
            int len = (int)(next - last);
            try {
                src.clear().position(last.intValue() + 12).limit(next.intValue() - 20);
                root.add(new WrappingChunk(src.slice()));
            } catch ( Throwable t ) {
                throw new AssertionError(t);
            }
            last = next;
        }
        assert(checkQueue(root));
        
        Collections.reverse(root);
//        root.set(0, new MappedChunk(src,root.get(0).getBuffers()));
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
            return false;
        }
        for (int x=0;x<got.size();x++) {
            if ( got.get(x).remaining() != list.get(x).remaining()  ) {
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
        List<Chunk> list = new ArrayList<Chunk>();
        Chunk buf = getBuffer();
        ByteBuffer[] chunk = readChunk(buf);

        while (chunk != null) {
            list.add(new MappedChunk(src, chunk));
            chunk = readChunk(buf);
        }
        if ( dir == Direction.REVERSE ) Collections.reverse(list); 
        
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
