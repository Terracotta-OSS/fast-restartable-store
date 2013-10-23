/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author mscott
 */
class MappedReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

    private final FileChannel source;
    private final   AppendableChunk                        data;
    private final   NavigableMap<Long,Marker>              boundaries;
    private final ReadWriteLock lock;
    private long offset = 0;
    
        
    public MappedReadbackStrategy(FileChannel src, Direction dir) throws IOException {
        this.source = src;
        MappedByteBuffer mapped = source.map(FileChannel.MapMode.READ_ONLY,0,(int)source.size());

        this.data = new AppendableChunk(new ByteBuffer[]{mapped});
        this.data.skip(source.position());
        boundaries = new TreeMap<Long,Marker>();
        createIndex();
        
        if ( !this.isConsistent() ) {
            lock = new ReentrantReadWriteLock();
        } else {
            lock = null;
        }
        
        if ( boundaries.isEmpty() ) {
            this.offset = Long.MIN_VALUE;
        } else if ( dir == Direction.REVERSE ) {
            this.offset = boundaries.lastKey();
        } else if ( dir == Direction.FORWARD ) {
            this.offset = boundaries.firstKey();
        } else {
            offset = Long.MIN_VALUE;
        }
    }

    @Override
    public long getMaximumMarker() {
        try {
            return boundaries.lastKey();
        } catch ( NoSuchElementException no ) {
            return Long.MIN_VALUE;
        }
    }

    private void createIndex() throws IOException {
        List<Long> jumps = readJumpList(data.getBuffers()[0]);
        if ( jumps == null )  {
            long start = data.position();
            ByteBuffer[] chunk = readChunk(data);
            while (chunk != null) {
                long marker = data.getLong(data.position() - 12);
                boundaries.put(marker,new Marker(start, marker));
                start = data.position();
                chunk = readChunk(data);
            }
            if ( this.isConsistent() ) {
                source.close();
            } else {
                data.truncate(start);
            }
        } else {
            Long last = data.position();
            for ( Long next : jumps ) {
                try {
                  long marker = data.getLong(next.intValue() - 12);
                  boundaries.put(marker,new Marker(last, marker));
                } catch ( Throwable t ) {
                    throw new AssertionError(t);
                }
                last = next;
            }
            source.close();
        }
    }
    
    private void updateIndex() throws IOException {
        long start = data.position();
        ByteBuffer[] chunk = readChunk(data);
        while (chunk != null) {
            long marker = data.getLong(data.position() - 12);
            boundaries.put(marker,new Marker(start, marker));
            start = data.position();
            chunk = readChunk(data);
        }
        if ( this.isConsistent() ) {
            data.destroy();
            data.append(source.map(FileChannel.MapMode.READ_ONLY, 0, source.size()));
            if ( data.remaining() < boundaries.lastEntry().getValue().getStart() ) {
                throw new AssertionError("bad boundaries " + data.remaining() + " " 
                        + boundaries.lastEntry().getValue().getStart());
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
        if ( !this.isConsistent() && data.position() != source.size() ) {            
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
        Map.Entry<Long,Marker> e = ( dir == Direction.FORWARD ) ? boundaries.ceilingEntry(offset) : boundaries.floorEntry(offset);
        key = e.getKey();
        return e.getValue().getChunk();
      } finally {
        offset = ( dir == Direction.FORWARD ) ? key + 1 : key - 1;
      }
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
        try {
            return ( dir == Direction.FORWARD ) ? ( boundaries.lastKey() >= offset ) : boundaries.firstKey() <= offset;
        } catch ( NoSuchElementException no ) {
            return false;
        }
    }
    
    public Chunk getBuffer() {
        return data.copy();
    }
    
    public long getLastMarker() {
        return boundaries.lastKey();
    }
    
    @Override
    public void close() throws IOException {
        if ( source.isOpen() ) {
            source.close();
        }
        boundaries.clear();
        data.destroy();
    }
        
    @Override
    public Chunk scan(long marker) throws IOException {
        Lock l = null;
        while ( !this.isConsistent() && boundaries.lastKey() < marker ) {
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
            Map.Entry<Long,Marker> m = boundaries.ceilingEntry(marker);
            if ( m == null ) {
                return null;
            } else {
                return m.getValue().getChunk();
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
        if ( value.getLong() != mark ) {
          throw new AssertionError("not valid");
        }
        return rv;
      }
      
    }
}
