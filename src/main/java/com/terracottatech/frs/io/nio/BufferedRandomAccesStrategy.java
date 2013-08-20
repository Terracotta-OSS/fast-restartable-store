/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
class BufferedRandomAccesStrategy extends AbstractReadbackStrategy implements Closeable {

    private final   FileBuffer buffer;
    private final   NavigableMap<Long,Marker>              boundaries;
    private volatile boolean sealed;
    private long offset = 0;
    private long length = 0;
    
    
        
    public BufferedRandomAccesStrategy(long startMark, FileBuffer buffer) throws IOException {
        this.buffer = buffer;
        this.offset = startMark;
        boundaries = new TreeMap<Long,Marker>();
        sealed = createIndex();
    }
    
    private synchronized boolean createIndex() throws IOException {
        FileBuffer data = getFileBuffer();
        int position = (int)Math.max(0,data.size() - data.capacity());
        data.position(position);
        int toRead = (int)Math.min(data.capacity(), data.size());
        data.partition(toRead);
        data.read(1);
        data.limit(toRead);
        List<Long> jumps = readJumpList(data.getBuffers()[0]);
        if ( jumps == null )  {
            length = NIOSegment.FILE_HEADER_SIZE;
            updateIndex();
            return false;
        } else {
            Long last = Long.valueOf(NIOSegment.FILE_HEADER_SIZE);
            for ( Long next : jumps ) {
                try {
                  data.position(next.intValue() - 12);
                  data.partition(8);
                  data.read(1);
                  long marker = data.getLong();
                  boundaries.put(marker,new Marker(last, marker));
                } catch ( Throwable t ) {
                    throw new AssertionError(t);
                }
                last = next;
            }
            length = data.size();
            return true;
        }
//        data.skip(NIOSegment.FILE_HEADER_SIZE);
    }   
    
    private FileBuffer getFileBuffer() {
        buffer.clear();
        return this.buffer;
    }

    @Override
    public long getMaximumMarker() {
        return boundaries.lastKey();
    }
    
    
    private synchronized void updateIndex() throws IOException {
        FileBuffer data = getFileBuffer();
        if ( length == data.size() ) {
            return;
        } else {
            data.position(length);
        }
        data.clear();
        long start = data.position();
        data.partition(4,8);
        data.read(1);
        int cs = data.getInt();
        while (SegmentHeaders.CHUNK_START.validate(cs)) {
            try {
                data.read(1);
                long len = data.getLong();
                data.position(data.position() + len);
                data.partition(20,4,8);
                data.read(1);
                if ( len != data.getLong() ) {
                    throw new IOException("chunk corruption - head and tail lengths do not match");
                }
                long marker = data.getLong();
                boundaries.put(marker,new Marker(start, marker));
                if ( !SegmentHeaders.FILE_CHUNK.validate(data.getInt()) ) {
                    throw new IOException("chunk corruption - file chunk magic is missing");
                } else {
                    start = data.position();
                }
                if ( data.position() < data.size() ) {
                    data.read(1);
                    cs = data.getInt();
                } else {
                    break;
                }
            } catch ( IOException ioe ) {
//  probably due to partial write completion, defer this marker until next round
                length = start;
            }
        }
        if ( SegmentHeaders.CLOSE_FILE.validate(cs) ) {
            sealed = true;
        }
        length = start;
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

    @Override
    public boolean isConsistent() {
      return true;
    }
    
    public long getLastMarker() {
        return boundaries.lastKey();
    }
    
    @Override
    public Chunk scan(long marker) throws IOException {
        if ( !sealed ) {
            updateIndex();
        }
        Map.Entry<Long,Marker> m = boundaries.ceilingEntry(marker);
        if ( m == null ) {
            return null;
        } else {
            return m.getValue().getChunk();
        }      
    }
    
    synchronized ByteBuffer readChunk(long position) throws IOException {
        FileBuffer data = getFileBuffer();
        data.position(position);
        data.partition(12);
        data.read(1);
        int cs = data.getInt(); 
        if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
          throw new AssertionError("not valid");
        }
        long len = data.getLong();
        if ( len > Integer.MAX_VALUE ) {
            throw new IOException("buffer overflow");
        }
        byte[] copy = new byte[(int)len];
        data.clear();
        while ( len > 0 ) {
            int grab = (int)Math.min(len, data.capacity());
            data.partition(grab);
            data.read(1);
            data.getBuffers()[0].get(copy, copy.length - (int)len, grab);
            len -= grab;
        }
        return ByteBuffer.wrap(copy);
    }    
    
    @Override
    public synchronized long size() throws IOException {
        return getFileBuffer().size();
    }
    
    @Override
    public synchronized void close() throws IOException {
        getFileBuffer().close();
    }
       
    private class Marker {
      private final long start;
      private final long mark;
      private SoftReference<ByteBuffer>  cache;

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
      
      public Chunk getChunk() throws IOException {
        ByteBuffer value = ( cache != null ) ? cache.get() : null;
        if ( value != null ) {
            return new WrappingChunk(value.asReadOnlyBuffer());
        }

        value = refreshCache();
        
        return new WrappingChunk(value.asReadOnlyBuffer());
      }
      
      private synchronized ByteBuffer refreshCache() throws IOException {
        ByteBuffer value = readChunk(start);
        cache = new SoftReference<ByteBuffer>(value);  // cached value, ok to race?
        return value;
      }
      
    }
}
