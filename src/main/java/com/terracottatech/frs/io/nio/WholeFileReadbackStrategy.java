/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
class WholeFileReadbackStrategy extends AbstractReadbackStrategy {
    
    private final FileBuffer                      buffer;
    private ListIterator<Chunk>   chunks;
    private Direction             queueDirection;
    
    
    public WholeFileReadbackStrategy(FileBuffer buffer) {
        super();
        this.buffer = buffer;
    }
    
    @Override
    public Iterator<Chunk> iterator() {
        try {
            if ( chunks == null ) queue(Direction.getDefault());
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
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
        buffer.read(1);
        
        List<Chunk> list = new ArrayList<Chunk>();
        ByteBuffer[] chunk = readChunk(buffer);

        while (chunk != null) {
            list.add(new WrappingChunk(chunk));
            chunk = readChunk(buffer);
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
