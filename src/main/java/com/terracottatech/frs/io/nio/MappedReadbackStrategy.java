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

import com.terracottatech.frs.io.AppendableChunk;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.util.LongLongOrderedDeltaArray.LongLongEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author mscott
 */
class MappedReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

    private final   FileChannel                           source;
    private final   AppendableChunk                        data;
    private final   MarkerDictionary              boundaries;
    private final ReadWriteLock lock;
    private final ChannelOpener opener;
    private long offset = 0;
    private long maxMarker = Long.MIN_VALUE;
        
    public MappedReadbackStrategy(FileChannel src, Direction dir, ChannelOpener opener) throws IOException {
      this.source = src;
      this.opener = opener;
        MappedByteBuffer mapped = src.map(FileChannel.MapMode.READ_ONLY,0,(int)src.size());

        this.data = new AppendableChunk(new ByteBuffer[]{mapped});
        this.data.skip(source.position());
        boundaries = new MarkerDictionary();
        createIndex(dir == Direction.RANDOM);
        
        if ( !this.isCloseDetected() ) {
            lock = new ReentrantReadWriteLock();
        } else {
            lock = null;
        }
        
        if ( boundaries.isEmpty() ) {
            this.offset = Long.MIN_VALUE;
        } else if ( dir == Direction.REVERSE ) {
            this.offset = boundaries.lastEntry().getKey();
        } else if ( dir == Direction.FORWARD ) {
            this.offset = boundaries.firstEntry().getKey();
        } else {
            offset = Long.MIN_VALUE;
        }
    }

  public MappedReadbackStrategy(FileChannel src, Direction dir) throws IOException {
      this(src, dir, null);
  }

    @Override
    public boolean isConsistent() {
      return super.isCloseDetected();
    }

    @Override
    public long getMaximumMarker() {
        return maxMarker;
    }

    private void createIndex(boolean full) throws IOException {
        long[] jumps = readJumpList(data.getBuffers()[0]);
        if ( jumps == null )  {
            long start = data.position();
            ByteBuffer[] chunk = readChunk(data);
            while (chunk != null) {
                long marker = data.getLong(data.position() - 12);
                updateMaxMarker(marker);
                boundaries.append(marker, start);
                start = data.position();
                chunk = readChunk(data);
            }
            if ( this.isCloseDetected() ) {
                source.close();
            } else {
                data.truncate(start);
            }
        } else {
            long last = data.position();
            long marker = 0;
            for ( long next : jumps ) {
                try {
 //  don't care about marker unless random access
                  if ( full ) {
                    marker = data.getLong(next - 12);
                  } else {
                    marker++;
                  }
                  updateMaxMarker(marker);
                  boundaries.append(marker, last);
                } catch ( Throwable t ) {
                    throw new AssertionError(t);
                }
                last = next;
            }
// replace the last item with the real marker number so getMaximumMarker works
            if ( !boundaries.isEmpty() ) {
              updateMaxMarker(data.getLong(last - 12));
            }
            source.close();
        }
    }

    private void updateMaxMarker(long marker) {
      if (marker > maxMarker) {
        maxMarker = marker;
      }
    }

  private void updateIndex() throws IOException {
        long start = data.position();
        ByteBuffer[] chunk = readChunk(data);
        while (chunk != null) {
            long marker = data.getLong(data.position() - 12);
            updateMaxMarker(marker);
            boundaries.append(marker, start);
            start = data.position();
            chunk = readChunk(data);
        }
        if ( this.isCloseDetected() ) {
            data.destroy();
            data.append(source.map(FileChannel.MapMode.READ_ONLY, 0, source.size()));
            if ( data.remaining() < boundaries.lastEntry().getValue() ) {
                throw new AssertionError("bad boundaries " + data.remaining() + " " 
                        + boundaries.lastEntry().getValue());
            }
            source.close();
        } else if ( data.hasRemaining() ) {
            data.truncate(start);
            if ( data.position() != start ) {
                throw new AssertionError("bad truncation");
            } 
            if ( data.length() != start ) {
                throw new AssertionError("bad truncation");
            }
        }
    }    
    
    private boolean addData() throws IOException {
        if ( !this.isCloseDetected() && data.position() != source.size() ) {            
            long len = data.position();
            if ( data.hasRemaining() ) {
                throw new AssertionError("bad tail");
            }
            MappedByteBuffer buffer = source.map(FileChannel.MapMode.READ_ONLY, len, source.size() - len);
            data.append(buffer);
            return true;
        }
        return false;
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
      Long key = null;
      try {
        LongLongEntry e = (dir == Direction.FORWARD) ? boundaries.ceilingEntry(offset) : boundaries.floorEntry(offset);
        key = e.getKey();
        long start = e.getValue();
        return getArbitraryChunkFromStart(start);
      } finally {
        offset = (dir == Direction.FORWARD) ? key + 1 : key - 1;
      }
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
      try {
        if (boundaries.isEmpty()) {
          return false;
        } else if (dir == Direction.FORWARD) {
          return boundaries.lastEntry().getKey() >= offset;
        } else {
          return boundaries.firstEntry().getKey() <= offset;
        }
      } catch (NoSuchElementException no) {
        return false;
      }
    }
    
    public Chunk getBuffer() {
        return data.copy();
    }
    
    @Override
    public void close() throws IOException {
      if (opener == null) {
        if (source.isOpen()) {
          source.close();
        }
      } else {
        opener.close();
      }
        boundaries.clear();
        data.destroy();
    }
        
    @Override
    public Chunk scan(long marker) throws IOException {
        Lock l = null;
        while ( !this.isCloseDetected() && boundaries.lastEntry().getKey() < marker ) {
            l = lock.writeLock();
            try {
                l.lock();
                if ( addData() ) {
                    updateIndex();
                }
            } finally {
                l.unlock();
            }
        } 
            
        if ( lock != null ) {
            l = lock.readLock();
        }
        
        try {
            if ( l != null ) {
                l.lock();
            }
            LongLongEntry m = boundaries.ceilingEntry(marker);
            if ( m == null ) {
                return null;
            } else {
                return getArbitraryChunkFromStart(m.getValue());
            } 
        } finally {
            if ( l != null ) {
                l.unlock();
            }
        }
    }
    
    @Override
    public long size() throws IOException {
        try {
            return source.size();
        } catch ( IOException ioe ) {
            return data.length();
        }
    }
       
    private class Marker {
      private final long start;
      private final long mark;

      public Marker(long start, long mark) {
        this.start = start;
        this.mark = mark;
      }

      public long getStart() {
        return start;
      }

      public long getMark() {
        return mark;
      }
      
      public Chunk getChunk() {
        Chunk value = data.copy();
        value.skip(start);
        int cs = value.getInt(); 
        if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
          throw new AssertionError("not valid");
        }
        long len = value.getLong();
        Chunk rv = value.getChunk(len);
        if ( value.getLong() != len ) {
          throw new AssertionError("not valid");
        }
//        if ( value.getLong() != mark ) {
//          throw new AssertionError("not valid");
//        }
        return rv;
      }
      
    }


  Chunk getArbitraryChunkFromStart(long start) {
    Chunk value = data.copy();
    value.skip(start);
    int cs = value.getInt();
    if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
      throw new AssertionError("not valid");
    }
    long len = value.getLong();
    Chunk rv = value.getChunk(len);
    if ( value.getLong() != len ) {
      throw new AssertionError("not valid");
    }
    return rv;
  }

}
