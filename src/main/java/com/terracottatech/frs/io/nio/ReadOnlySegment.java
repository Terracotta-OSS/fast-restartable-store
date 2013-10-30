/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
class ReadOnlySegment extends NIOSegment implements Closeable {
//  for reading 
    private FileChannel source;
    private final NIOAccessMethod method;
    private ReadbackStrategy strategy;
    private final Direction dir;
    private volatile long length = 0;
    
    ReadOnlySegment(NIOStreamImpl parent, NIOAccessMethod strat, File buffer, Direction dir) throws IOException, HeaderException {
        super(parent,buffer);
        this.dir = dir;
        this.method = strat;
    }
    
    public synchronized ReadOnlySegment load() throws IOException {
        if ( strategy == null ) {
            try {
                if ( dir == Direction.RANDOM ) {
                    strategy = openForRandomAccess();
                } else {
                    strategy = openForReplay();
                }
            } catch ( HeaderException h ) {
                throw new IOException(h);
            }
        }
        return this;
    }

    private ReadbackStrategy openForReplay() throws IOException, HeaderException {
        source = new FileInputStream(getFile()).getChannel();
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }
        
        FileBuffer buffer = (getStream() != null) ? getStream().createFileBuffer(source, 8192) :
                new FileBuffer(source, ByteBuffer.allocate(8192));
        buffer.partition(FILE_HEADER_SIZE);
        buffer.read(1);
        readFileHeader(buffer);
            
        if ( method == NIOAccessMethod.MAPPED ) {
            return new MappedReadbackStrategy(buffer, Direction.REVERSE);
        } else if ( method == NIOAccessMethod.STREAM ) {
            return new BufferedReadbackStrategy(Direction.REVERSE, buffer);
        } else {
            throw new RuntimeException("unrecognized readback method");
        }
    }
    
    private ReadbackStrategy openForRandomAccess() throws IOException, HeaderException {
        source = new FileInputStream(getFile()).getChannel();
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }
        
        FileBuffer buffer = (getStream() != null) ? getStream().createFileBuffer(source, 64) :
                new FileBuffer(source, ByteBuffer.allocate(8192));
        buffer.partition(FILE_HEADER_SIZE);
        buffer.read(1);
        readFileHeader(buffer);
            
        if ( method == NIOAccessMethod.MAPPED ) {
            return new MappedReadbackStrategy(buffer, Direction.RANDOM); 
        } else if ( method == NIOAccessMethod.STREAM ) {
            return new BufferedReadbackStrategy(Direction.RANDOM, buffer);
        } else {
            throw new RuntimeException("unrecognized readback method");
        }
    }    
     
    public Chunk scan(long marker) throws IOException {
        return strategy.scan(marker);
    }   
     
    public long getMaximumMarker() throws IOException {
        return strategy.getMaximumMarker();
    }     
        
    public boolean isClosed() {
        return ( strategy == null );
    }
    
    @Override
    public void close() throws IOException {
        if ( isClosed() ) {
            return;
        }
        if ( strategy instanceof Closeable ) {
            ((Closeable)strategy).close();
        } else {
          source.close();
        }
        strategy = null;
        source = null;
    }

  @Override
  public String toString() {
    return "ReadOnlySegment{" + "strategy=" + strategy + '}';
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
