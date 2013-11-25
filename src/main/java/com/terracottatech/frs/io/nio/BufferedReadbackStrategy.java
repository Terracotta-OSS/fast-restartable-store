/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
class BufferedReadbackStrategy extends BaseBufferReadbackStrategy {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ReadbackStrategy.class);
    private final   NavigableMap<Long,Marker>              boundaries;
    private final   ReentrantReadWriteLock                          block;
    private long    lastKey = Long.MIN_VALUE;
    private volatile boolean sealed;
    private long offset = 0;
    private long length = 0;    
    
    public BufferedReadbackStrategy(Direction dir, FileChannel channel, BufferSource source) throws IOException {
        super(dir,channel,source);
        boundaries = new TreeMap<Long,Marker>();
        length = channel.position();
        sealed = createIndex(dir == Direction.RANDOM);
        if ( !sealed ) {
          block = new ReentrantReadWriteLock();
        } else {
          block = null;
        }
        if ( boundaries.isEmpty() ) {
            this.offset = 0;
        } else if ( dir == Direction.REVERSE ) {
            this.offset = boundaries.lastKey();
        } else if ( dir == Direction.FORWARD ) {
            this.offset = boundaries.firstKey();
        } else {
            offset = Long.MIN_VALUE;
        }
    }

    @Override
    public boolean isConsistent() {
      return sealed;
    }
    
    @Override
    public long getMaximumMarker() {
      return lastKey;
    }    
    
    private boolean createIndex(boolean full) throws IOException {
        long[] jumps = null;
        long capacity = getChannel().size();
        if ( capacity > 8192 ) {
          ByteBuffer buffer = allocate(8);
          int num;
          int jump;
          try {
            readDirect(capacity - 8, buffer);
            num = buffer.getInt();
            jump = buffer.getInt();
          } finally {
            free(buffer);
          }
          if ( num >= 0 && SegmentHeaders.JUMP_LIST.validate(jump) ) {
            int stretch = (num * ByteBufferUtils.INT_SIZE) + 8 + ByteBufferUtils.INT_SIZE;
            if ( stretch < capacity ) {
              ByteBuffer grab = allocate(stretch);
              if ( grab != null ) {
                try {
                  readDirect(capacity - stretch, grab);
                  jumps = readJumpList(grab);
                } finally {
                  free(grab);                   
                }
              }
            }
          }
        }
        
        if ( jumps == null )  {
            return updateIndex();
        } else {
            ByteBuffer buffer = allocate(16);
            try {
              final long first = getChannel().position();
              if ( full || jumps.length == 0 ) {
                long last = first;
                buffer.mark();
                for ( long next : jumps ) {
                    readDirect(next - 20, buffer);
                    long clen = buffer.getLong();
                    long marker = buffer.getLong();
                    if ( last != next - 20 - clen - 12) {
                      throw new AssertionError("bad start position");
                    }
                    boundaries.put(marker,new Marker(last, marker));
                    last = next;
                    buffer.reset();
                }
              } else {
 //  don't care about the lsn for iteration so cheat, only need the last one
                readDirect(jumps[jumps.length-1] - 20, buffer);
                long clen = buffer.getLong();
                lastKey = buffer.getLong();
                long start = first;
                long marker = 0;
                for (int x=0;x<jumps.length;x++) {
                  boundaries.put(marker,new Marker(start, marker++, jumps[x] - start - 20));
                  start = jumps[x];
                }
              }
            } finally {
              free(buffer);
            }
            length = getChannel().size();
            lastKey = ( boundaries.isEmpty() ) ? Long.MIN_VALUE : boundaries.lastKey();
            return true;
        }
    }  
    
    private boolean updateIndex() throws IOException {
      FileChannel channel = getChannel();
      
        if ( length + 4 > getChannel().size() ) {
            return sealed;
        } else {
            channel.position(length);
        }
        long start = channel.position();
        ByteBuffer buffer = allocate(32);
        int b = buffer.position();
        int e = buffer.limit();
        int chunkStart = 0;
        try {
          try {
              readFully(4, buffer);
          } catch ( IOException ioe ) {
              System.out.println("bad length " + length + " " + channel.position() + " " + channel.size());
              throw ioe;
          }
          chunkStart = buffer.getInt();
          while (SegmentHeaders.CHUNK_START.validate(chunkStart)) {
              try {
                  readFully(8, buffer);
                  long len = buffer.getLong();
                  channel.position(channel.position() + len);
                  readFully(20, buffer);
                  if ( len != buffer.getLong() ) {
                      throw new IOException("chunk corruption - head and tail lengths do not match");
                  }
                  long marker = buffer.getLong();
                  boundaries.put(marker,new Marker(start, marker));
                  if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt()) ) {
                      throw new IOException("chunk corruption - file chunk magic is missing");
                  } else {
                      start = getChannel().position();
                  }
                  buffer.position(b).limit(e);
                  if ( channel.position() < channel.size() ) {
                      readFully(4, buffer);
                      chunkStart = buffer.getInt();
                  } else {
                      break;
                  }
              } catch ( IOException ioe ) {
  //  probably due to partial write completion, defer this marker until next round
                  length = start;
                  break;
              }
          }
        } finally {
            free(buffer);
        }
        if ( SegmentHeaders.CLOSE_FILE.validate(chunkStart) ) {
            sealed = true;
        }
        length = start;
        lastKey = ( boundaries.isEmpty() ) ? Long.MIN_VALUE : boundaries.lastKey();
        return sealed;
    }     

    @Override
    public Chunk iterate(Direction dir) throws IOException {
      Long key = null;
      try {
        Map.Entry<Long,Marker> e = ( dir == Direction.FORWARD ) ? boundaries.ceilingEntry(offset) : boundaries.floorEntry(offset);
        if (e == null ) {
            return null;
        }
        key = e.getKey();
        return e.getValue().getChunk();
      } finally {
        if ( key != null ) {
            offset = ( dir == Direction.FORWARD ) ? key + 1 : key - 1;
        }
      }
    }

    @Override
    public boolean hasMore(Direction dir) throws IOException {
      try {
          return ( dir == Direction.FORWARD ) ? ( lastKey >= offset ) : (boundaries.firstKey() <= offset );
      } catch ( NoSuchElementException no ) {
          return false;
      }
    }
    
    @Override
    public Chunk scan(long marker) throws IOException {
      Lock lock = null;
      try {
        if ( !sealed ) {
          lock = block.readLock();
          lock.lock();
          while ( marker > lastKey && !sealed ) {
              // upgrade lock
              lock.unlock();
              Lock writer = block.writeLock();
              try {
                writer.lock();
                updateIndex();
              } finally {
                writer.unlock();
                lock.lock();
              }
          }
        }
        Map.Entry<Long,Marker> m = boundaries.ceilingEntry(marker);
        if ( m == null ) {
            return null;
        } else {
            return m.getValue().getChunk();
        }  
      } finally {
        if ( lock != null ) {
          lock.unlock();
        }
      }
    }
}
