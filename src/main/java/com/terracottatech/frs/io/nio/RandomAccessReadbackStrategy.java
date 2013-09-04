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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author mscott
 */
class RandomAccessReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

    
    private final FileChannel source;
    private final   AppendableChunk                        data;
    private final   NavigableMap<Long,Marker>              boundaries;
    private final ReadWriteLock lock;
    private long offset = 0;
    
        
    public RandomAccessReadbackStrategy(long startMark, FileChannel src, Direction direction) throws IOException {
        this.source = src;
        MappedByteBuffer mapped = source.map(FileChannel.MapMode.READ_ONLY,0,(int)source.size());

        if ( direction != Direction.RANDOM ) {
        throw new AssertionError("expected random access");
        }
        this.data = new AppendableChunk(new ByteBuffer[]{mapped});
        this.offset = startMark;
        boundaries = new TreeMap<Long,Marker>();
//        boundaries = new ConcurrentSkipListMap<Long, Marker>();
        data.skip(NIOSegment.FILE_HEADER_SIZE);
        createIndex();
        
        if ( !this.isConsistent() ) {
            lock = new ReentrantReadWriteLock();
        } else {
            lock = null;
        }
    }

    @Override
    public long getMaximumMarker() {
        return boundaries.lastKey();
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
            Long last = Long.valueOf(NIOSegment.FILE_HEADER_SIZE);
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
    
    private void addData() throws IOException {
        if ( !this.isConsistent() && data.position() != source.size() ) {            
            long len = data.position();
            if ( data.hasRemaining() ) {
                throw new AssertionError("bad tail");
            }
            MappedByteBuffer buffer = source.map(FileChannel.MapMode.READ_ONLY, len, source.size() - len);
            data.append(buffer);
        }
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
      Long key = null;
      try {
        Map.Entry<Long,Marker> e = ( dir == Direction.FORWARD ) ? boundaries.higherEntry(offset) : boundaries.lowerEntry(offset);
        key = e.getKey();
        return e.getValue().getChunk();
      } finally {
        offset = key;
      }
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
      return ( boundaries.lastKey() >= offset );
    }
    
    public Chunk getBuffer() {
        return data.copy();
    }
    
    public long getLastMarker() {
        return boundaries.lastKey();
    }
    
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
        if ( source.isOpen() && boundaries.lastKey() < marker ) {
            l = lock.writeLock();
            l.lock();
            if ( boundaries.lastKey() < marker && this.source.isOpen() ) {
                addData();
                updateIndex();
            } else {
                l.unlock();
                l = lock.readLock();
                l.lock();
            }
            if ( boundaries.lastKey() < marker && !this.isConsistent() ) {
                throw new AssertionError(boundaries.lastKey() + " < " + marker);
            }
        } else if ( lock != null ) {
            l = lock.readLock();
            l.lock();
        }
        
        try {
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
    public long size() {
        return data.length();
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
