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

/**
 *
 * @author mscott
 */
class RandomAccessReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

    
    private final FileChannel source;
    private final   AppendableChunk                        data;
    private final   NavigableMap<Long,Marker>              boundaries;
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
        data.skip(NIOSegment.FILE_HEADER_SIZE);
        createIndex();
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
        data.clear();
//        data.skip(NIOSegment.FILE_HEADER_SIZE);
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
            source.close();
        }
    }    
    
    private synchronized void addData() throws IOException {
        if ( this.isConsistent() || data.length() == source.size() ) {
            return;
        }
        long len = data.length();
        MappedByteBuffer buffer = source.map(FileChannel.MapMode.READ_ONLY, len, source.size() - len);
        data.append(buffer);
        data.clear();
        data.skip(len);
        updateIndex();
        data.clear();
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
      try {
        return boundaries.ceilingEntry(offset).getValue().getChunk();     
      } finally {
        offset += dir == dir.REVERSE ? -1 : 1;
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
    
    public synchronized void close() throws IOException {
        if ( source.isOpen() ) {
            source.close();
        }
        boundaries.clear();
        data.destroy();
    }
    
    @Override
    public Chunk scan(long marker) throws IOException {
        if ( !this.isConsistent() ) {
            addData();
        }
        Map.Entry<Long,Marker> m = boundaries.ceilingEntry(marker);
        if ( m == null ) {
            return null;
        } else {
            return m.getValue().getChunk();
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
        return value.getChunk(len);
      }
      
    }
}
