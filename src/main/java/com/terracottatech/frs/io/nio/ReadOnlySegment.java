/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.SimpleBufferSource;
import com.terracottatech.frs.io.WrappingChunk;

import java.io.Closeable;
import java.io.File;
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
    private final FileChannelReadOpener opener;
    private final NIOAccessMethod method;
    private ReadbackStrategy strategy;
    private final Direction dir;
    private volatile long length = 0;
    
    ReadOnlySegment(NIOStreamImpl parent, NIOAccessMethod strat, File buffer, Direction dir) throws IOException, HeaderException {
        super(parent,buffer);
        this.dir = dir;
        this.method = strat;
        this.opener = new FileChannelReadOpener(getFile());
    }
    
    public synchronized ReadOnlySegment load(BufferSource src) throws IOException {
        if ( strategy == null ) {
          if ( src == null ) {
            src = new SimpleBufferSource();
          }
            try {
                if ( dir == Direction.RANDOM ) {
                    strategy = openForRandomAccess(src);
                } else {
                    strategy = openForReplay(src);
                }
            } catch ( HeaderException h ) {
                throw new IOException(h);
            }
        }
        return this;
    }

  private void readFullyFirstBytes(ByteBuffer buffer) throws IOException {
    int startPosition = buffer.position();
    while (buffer.hasRemaining()) {
      try {
        source.read(buffer);
      } catch (PositionLostException e) {
        source.position(buffer.position() - startPosition);
      }
    }
  }

    private ReadbackStrategy openForReplay(BufferSource src) throws IOException, HeaderException {
        source = new WrappedFileChannel(opener.open(), opener);
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }
        
        ByteBuffer header = src.getBuffer(FILE_HEADER_SIZE);
        readFullyFirstBytes(header);
        header.flip();
        readFileHeader(new WrappingChunk(header));
        src.returnBuffer(header);

        while (true) {
          try {
            if (method == NIOAccessMethod.MAPPED) {
              return new MappedReadbackStrategy(source, Direction.REVERSE, opener);
            } else if (method == NIOAccessMethod.STREAM) {
              return new MinimalReadbackStrategy(Direction.REVERSE, getMinimumMarker(), source, src, opener);
            } else {
              throw new RuntimeException("unrecognized readback method");
            }
          } catch (PositionLostException e) {
            source.position(FILE_HEADER_SIZE);
          }
        }
    }
    
    private ReadbackStrategy openForRandomAccess(BufferSource src) throws IOException, HeaderException {
        source = new WrappedFileChannel(opener.open(), opener);
        length = source.size();

        if (length < FILE_HEADER_SIZE) {
            throw new HeaderException("bad header", this);
        }
        
        ByteBuffer header = src.getBuffer(FILE_HEADER_SIZE);
        readFullyFirstBytes(header);
        header.flip();
        readFileHeader(new WrappingChunk(header));
        src.returnBuffer(header);

        while (true) {
          try {
            if (method == NIOAccessMethod.MAPPED) {
              return new MappedReadbackStrategy(source, Direction.RANDOM, opener);
            } else if (method == NIOAccessMethod.STREAM) {
              return new MinimalReadbackStrategy(Direction.RANDOM, getMinimumMarker(), source, src, opener);
            } else {
              throw new RuntimeException("unrecognized readback method");
            }
          } catch (PositionLostException ple) {
            source.position(FILE_HEADER_SIZE);
          }
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
    
    public boolean isComplete() {
      return strategy.isConsistent();
    }
    
    @Override
    public void close() throws IOException {
        if ( isClosed() ) {
            return;
        }
        if ( strategy instanceof Closeable ) {
            ((Closeable)strategy).close();
        } else {
          opener.close();
        }
        strategy = null;
        source = null;
    }

  @Override
  public String toString() {
    return "ReadOnlySegment{"+ "strategy=" + strategy + "}\n\t" + super.toString();
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
