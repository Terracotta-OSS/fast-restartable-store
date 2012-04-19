/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.*;

/**
 *
 * @author mscott
 */
class MappedReadbackStrategy extends AbstractReadbackStrategy {

    MappedFileBuffer                      src;
    protected ListIterator<Chunk>   chunks;
    protected Direction             queueDirection;
    
    public MappedReadbackStrategy(FileChannel data) throws IOException {
        src = new MappedFileBuffer(data.map(MapMode.READ_ONLY, 0, data.size()));
    }
    
    public Chunk getBuffer() {
        return src;
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
        ByteBuffer[] chunk = readChunk(src);

        while (chunk != null) {
            list.add(new WrappingChunk(chunk));
            chunk = readChunk(src);
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
