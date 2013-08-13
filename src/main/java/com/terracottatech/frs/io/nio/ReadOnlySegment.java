/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
class ReadOnlySegment extends NIOSegment implements Closeable {
//  for reading 
    private FileChannel source;
    private ReadbackStrategy strategy;
    private volatile long length = 0;
    
    ReadOnlySegment(NIOStreamImpl parent, File buffer, Direction dir) throws IOException, HeaderException {
        super(parent,buffer);
        if ( dir == Direction.RANDOM ) {
            strategy = openForRandomAccess();
        } else {
            strategy = openForReplay();
        }
    }

    private ReadbackStrategy openForReplay() throws IOException, HeaderException {
        source = new FileInputStream(getFile()).getChannel();
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }

        MappedByteBuffer buf = source.map(FileChannel.MapMode.READ_ONLY,0,(int)length);
        buf.load();
        readFileHeader(new WrappingChunk(buf));
                
        return new MappedReadbackStrategy(buf,Direction.REVERSE);
    }
    
    private synchronized ReadbackStrategy openForRandomAccess() throws IOException, HeaderException {
        source = new FileInputStream(getFile()).getChannel();
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }

//        RandomAccessReadbackStrategy rr = new RandomAccessReadbackStrategy(this.getBaseMarker(), source, Direction.RANDOM); 
//        readFileHeader(rr.getBuffer());
//        return rr;
        
        FileBuffer buffer = new FileBuffer(source, ByteBuffer.allocate(1024));
        buffer.partition(FILE_HEADER_SIZE);
        buffer.read(1);
        readFileHeader(buffer);

        return new BufferedRandomAccesStrategy(this.getBaseMarker(),buffer);
    }    
     
    public Chunk scan(long marker) throws IOException {
        return strategy.scan(marker);
    }   
        
    public boolean isClosed() {
        return ( strategy == null );
    }
    
    @Override
    public void close() throws IOException {
        if ( isClosed() ) {
            return;
        }
        source.close();
        if ( strategy instanceof Closeable ) {
            ((Closeable)strategy).close();
        }
        strategy = null;
        source = null;
    }

    public Chunk next(Direction dir) throws IOException {
        if (strategy.hasMore(dir)) {
            return strategy.iterate(dir);
        }
        throw new IOException("segment bounds");
    }

    public boolean hasMore(Direction dir) throws IOException {
        return strategy.hasMore(dir);
    }
    
    @Override
    public long size() {
        try {
            return strategy.size();
        } catch ( IOException ioe ) {
            return -1;
        }
    }
}
