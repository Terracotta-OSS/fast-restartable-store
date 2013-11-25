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
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
class MinimalReadbackStrategy extends BaseBufferReadbackStrategy {

    private static final        Logger LOGGER = LoggerFactory.getLogger(ReadbackStrategy.class);
//    private long[]              jumpList;
//    private final Map<Integer,Long>              markerCache = Collections.synchronizedMap(new LinkedHashMap<Integer,Long>(256,.75f,true) {
//
//      @Override
//      protected boolean removeEldestEntry(Map.Entry<Integer, Long> eldest) {
//        return size() > 256;
//      }
//      
//    });
    private final MarkerIndex                       index;
    private final ReentrantReadWriteLock                          block;
    private volatile boolean    sealed;
    private final long          firstKey;
    private long                lastKey = Long.MAX_VALUE;
    private long                length = 0; 
    private final long          start;
    private int                 position = Integer.MIN_VALUE;
        
    public MinimalReadbackStrategy(Direction dir, long first, FileChannel channel, BufferSource source) throws IOException {
      super(dir,channel,source);
      this.firstKey = first;
      length = channel.position();
      start = length;
      index = new MarkerIndex(source);
      sealed = createIndex();
      if ( !sealed ) {
        block = new ReentrantReadWriteLock();
      } else {
        block = null;
      }
      if ( dir == Direction.REVERSE ) {
        this.position = index.size();
      } else {
        this.position = 0;
      }
    }

    @Override
    public boolean isConsistent() {
      return sealed;
    }
    
    private long locate(long mark) throws IOException {
      if ( mark > lastKey ) {
        return Long.MIN_VALUE;
      }
      if ( mark < firstKey ) {
        throw new AssertionError();
      }
      
      int low = 0;
      int high = index.size() - 1;
      int cur = 0;
      
      ByteBuffer buf = allocate(8);
      buf.mark();
      try {
        long comp = -1;
        long lowmark = -1;
        while ( comp != mark ) {
          if ( comp < mark ) {
            low = cur;
            lowmark = comp;
            cur = low + ((high - low) / 2);
          } else {
            high = cur;
            cur = high - ((high - low) / 2);
          }
          if ( high - low <= 1 ) {
            if ( lowmark < 0 ) {
              buf.reset();
              lowmark = readMark(low,buf);
            }
            if ( mark <= lowmark ) {
              cur = low;
            } else {
              cur = high;
            }
            break;
          }
          buf.reset();
          comp = readMark(cur,buf);
        }
        buf.reset();
        if ( readMark(cur,buf) < mark ) {
          throw new AssertionError();
        }
        buf.reset();
        if ( cur > 0 && readMark(cur-1,buf) > mark ) {
          throw new AssertionError();
        }
        return (cur == 0 ) ? start : index.position(cur-1);
      } finally {
        free(buf);
      }
    }
    
    private long readMark(int pos, ByteBuffer buf) throws IOException {
      ByteBuffer rsrc = buf;
      long marker = index.mark(pos);
      if ( marker > 0 ) {
        return marker;
      }
      if ( rsrc == null ) {
        rsrc = allocate(8);
      }
      try {
        readDirect(index.position(pos) - 12, rsrc);
        marker = rsrc.getLong();
        index.cache(pos,marker);
        return marker;
      } finally {
        if ( buf == null ) {
          free(rsrc);
        }
      }
    }
        
    private boolean createIndex() throws IOException {
      FileChannel channel = getChannel();
      long[] jumpList = null;
        long capacity = channel.size();
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
                  jumpList = readJumpList(grab);
                } finally {
                  free(grab);                   
                }
              }
            }
          }
        }
        
        if ( jumpList == null )  {
            return updateIndex();
        } else {
            index.append(jumpList);
            ByteBuffer buffer = allocate(16);
            try {
                lastKey = readMark(jumpList.length-1, buffer);
            } finally {
              free(buffer);
            }
            length = channel.size();
            return true;
        }
    }  

    @Override
    public long getMaximumMarker() {
      return lastKey;
    } 
    
    private boolean updateIndex() throws IOException {
      FileChannel channel = getChannel();
        if ( length + 4 > channel.size() ) {
            return sealed;
        } else {
            channel.position(length);
        }
        ByteBuffer buffer = allocate(32);
        int b = buffer.position();
        int e = buffer.limit();
        int chunkStart = 0;
        long last = channel.position();
        try {
          try {
              readFully(4, buffer);
          } catch ( IOException ioe ) {
              System.out.println("bad length " + length + " " + channel.position() + " " + channel.size());
              throw ioe;
          }
          chunkStart = buffer.getInt();
          ArrayList<Long> list = new ArrayList<Long>();
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
                  list.add(channel.position());
                  if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt()) ) {
                      throw new IOException("chunk corruption - file chunk magic is missing");
                  }
                  buffer.position(b).limit(e);
                  last = channel.position();
                  if ( channel.position() < channel.size() ) {
                      readFully(4, buffer);
                      chunkStart = buffer.getInt();
                  } else {
                      break;
                  }
              } catch ( IOException ioe ) {
  //  probably due to partial write completion, defer this marker until next round
                  length = last;
                  break;
              }
          }
          int pos = 0;
          index.append(list);
        } finally {
            free(buffer);
        }
        if ( SegmentHeaders.CLOSE_FILE.validate(chunkStart) ) {
            sealed = true;
        }
        length = last;
        lastKey = readMark(index.size() - 1,null);
        return sealed;
    }     

    @Override
    public boolean hasMore(Direction dir) throws IOException {
        return ( dir == Direction.FORWARD ) ? ( position < index.size()) : ( position > 0 );
    }

    @Override
    public Chunk iterate(Direction dir) throws IOException {
      if ( dir == Direction.REVERSE) {
        position -= 1;
      }
      try {
        long begin = ( position == 0 ) ? start : index.position(position-1);
        return new VirtualChunk(begin,index.position(position) - begin - 20);
      } finally {
        if ( dir == Direction.FORWARD ) {
          position += 1;
        }
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
      long pos = locate(marker);
      if ( pos < 0 ) {
        return null;
      }
      return new VirtualChunk(pos);
      } finally {
        if ( lock != null ) {
          lock.unlock();
        }
      }
    }

  @Override
  public void close() throws IOException {
    super.close(); 
    index.close();
  }
  
}
