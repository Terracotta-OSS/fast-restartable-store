/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
class BufferedReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadbackStrategy.class);
    private final   FileChannel channel;
    private final   BufferSource source;
    private final   NavigableMap<Long,Marker>              boundaries;
    private final   ReentrantReadWriteLock                          block;
    private volatile boolean sealed;
    private long    lastKey;
    private long offset = 0;
    private long length = 0;    
    private final AtomicInteger  outchunks = new AtomicInteger();
    private volatile boolean closeRequested = false;
    private static final ByteBuffer[] EMPTY = new ByteBuffer[] {};
    
    public BufferedReadbackStrategy(Direction dir, FileChannel channel, BufferSource source) throws IOException {
        this.channel = channel;
        this.source = source;
        boundaries = new TreeMap<Long,Marker>();
        length = channel.position();
        sealed = createIndex(dir == Direction.RANDOM);
        if ( !sealed ) {
          block = new ReentrantReadWriteLock();
        } else {
          block = null;
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
    public boolean isConsistent() {
      return sealed;
    }
    
    private boolean createIndex(boolean full) throws IOException {
        List<Long> jumps = null;
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
              if ( full || jumps.isEmpty() ) {
                for ( Long next : jumps ) {
                    readDirect(next.intValue() - 20, buffer);
                    long clen = buffer.getLong();
                    long marker = buffer.getLong();
                    boundaries.put(marker,new Marker(next - 20 - clen - 12, marker));
                    buffer.rewind();
                }
              } else {
 //  don't care about the lsn for iteration so cheat, only need the last one
                long first = channel.position();
                Collections.reverse(jumps);
                ListIterator<Long> jl = jumps.listIterator();
                long last = jl.next();
                readDirect(last - 20, buffer);
                long clen = buffer.getLong();
                long marker = buffer.getLong();
                while ( jl.hasNext() ) {
                  long current = jl.next();
                  boundaries.put(marker,new Marker(current, marker, last - current - 20));
                  marker -= 1;
                  last = current;
                }
                boundaries.put(marker, new Marker(first, marker, last - NIOSegment.FILE_HEADER_SIZE - 20));
              }
            } finally {
              free(buffer);
            }
            length = channel.size();
            return true;
        }
    }  
    
    private void addChunk(Chunk c) throws IOException {
      if ( !this.channel.isOpen() ) {
        throw new IOException("file closed");
      }
      outchunks.incrementAndGet();
    }
    
    private void removeChunk(Chunk c) throws IOException {
      if ( outchunks.decrementAndGet() == 0 && closeRequested ) {
        this.channel.close();
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
    
    private boolean updateIndex() throws IOException {
        if ( length + 4 > channel.size() ) {
            return sealed;
        } else {
            channel.position(length);
        }
        long start = channel.position();
        ByteBuffer buffer = allocate(32);
        try {
            readFully(4, buffer);
        } catch ( IOException ioe ) {
            System.out.println("bad length " + length + " " + channel.position() + " " + channel.size());
            throw ioe;
        }
        int cs = buffer.getInt();
        while (SegmentHeaders.CHUNK_START.validate(cs)) {
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
                    start = channel.position();
                }
                if ( channel.position() < channel.size() ) {
                    readFully(4, buffer);
                    cs = buffer.getInt();
                } else {
                    break;
                }
            } catch ( IOException ioe ) {
//  probably due to partial write completion, defer this marker until next round
                length = start;
                break;
            } finally {
                free(buffer);
            }
        }
        if ( SegmentHeaders.CLOSE_FILE.validate(cs) ) {
            sealed = true;
        }
        length = start;
        lastKey = boundaries.lastKey();
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
          return ( dir == Direction.FORWARD ) ? ( boundaries.lastKey() >= offset ) : (boundaries.firstKey() <= offset );
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
    
    private ByteBuffer readFully(int amount, ByteBuffer get) throws IOException {
        get.limit(amount).rewind();
        while ( get.hasRemaining() ) {
            channel.read(get);
        }
        get.flip(); 
        return get;
    }
    
    private ByteBuffer readDirect(long position, ByteBuffer get) throws IOException {
        int read = 0;
        while ( get.hasRemaining() ) {
            read += channel.read(get,position + read);
        }
        get.flip(); 
        return get;
    }      
    
    private int writeDirect(long position, ByteBuffer get) throws IOException {
        throw new UnsupportedOperationException("read only");
    }    
    
    private ByteBuffer readVirtualDirect(long positon, ByteBuffer get) {
        try {
            return readDirect(positon, get);
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        } 
    }
    
    private ByteBuffer allocate(int size) {
      if ( size == 0 ) {
        throw new AssertionError();
      }
      
      if ( source == null ) {
        return ByteBuffer.allocate(size);
      }
          
      return source.getBuffer(size);
    }
    
    private void free(ByteBuffer buf) {
      if ( source != null ) {
        source.returnBuffer(buf);
      }
    }

  @Override
  public String toString() {
    return "BufferedReadbackStrategy{" + "openchunks=" + outchunks + ", closeRequested=" + closeRequested + '}';
  }
    
    private int writeVirtualDirect(long positon, ByteBuffer put) {
        try {
            return writeDirect(positon, put);
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
    }    
    
    @Override
    public long size() throws IOException {
        return this.channel.size();
    }
    
    @Override
    public void close() throws IOException {
      closeRequested = true;
      if ( outchunks.get() == 0 ) {
        this.channel.close();
      }
    }
       
    private class Marker {
      private final long start;
      private final long mark;
      private final long lhint;

      public Marker(long start, long mark) {
        this.start = start;
        this.mark = mark;
        this.lhint = 0;
      }
      
      public Marker(long start, long mark, long lguess) {
        this.start = start;
        this.mark = mark;
        this.lhint = lguess;
      }         

      public long getStart() {
        return start;
      }

      public long getMark() {
        return mark;
      }
      
      public Chunk getFullChunk() throws IOException {
        FullChunk header = new FullChunk(start, 12);
        int cs = header.getInt();
        if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
          throw new AssertionError("not valid");
        }
        long len = header.getLong();
        if ( len > Integer.MAX_VALUE ) {
            throw new IOException("buffer overflow");
        }
        header.close();
        return new FullChunk(start + 12, len);
      }
       
      public VirtualChunk getChunk() throws IOException {
          if ( closeRequested ) {
            throw new IOException("file closed");
          }
          VirtualChunk c = new VirtualChunk(start);
          long len = lhint;
          if ( len == 0 ) {
            Chunk header = c.getChunk(12);
            int cs = header.getInt(); 
            if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
              throw new AssertionError("not valid");
            }
            len = header.getLong() + 12;
            if ( len > Integer.MAX_VALUE ) {
                throw new IOException("buffer overflow");
            }
            if ( header instanceof Closeable ) {
                ((Closeable)header).close();
            }
          } else {
            c.skip(12);
          }
          c.setLength(len);
          return c;
      }
    }
    
    private class FullChunk extends AbstractChunk implements Closeable {
        
        private final ByteBuffer[] data;
        private volatile boolean closed = false;
        private final AtomicInteger openCount = new AtomicInteger(1);
        
        public FullChunk(long offset, long size) throws IOException {
          if ( size == 0 ) {
            data = EMPTY;
          } else {
            data = new ByteBuffer[] {readVirtualDirect(offset, allocate((int)size))};
          }
          register();
        }
        
        private void register() throws IOException {
          addChunk(this);
        }
        
        @Override
        public ByteBuffer[] getBuffers() {
            return data;
        }
        
        @Override
        public Chunk getChunk(long size) {
          final ByteBuffer[] sub = this.getBuffers(size);
          final Closeable parent = this;
          openCount.incrementAndGet();
          return new CloseableChunk() {
            boolean closed = false;

            @Override
            public ByteBuffer[] getBuffers() {
              return sub;
            }

            @Override
            public void close() throws IOException {
              closed = true;
              parent.close();
            }
//  ONLY FOR DEBUGGING LEAKS
//            @Override
//            protected void finalize() throws Throwable {
//              super.finalize(); 
//              if ( !closed ) {
//                LOGGER.warn("LEAK DETECTED");
//                close();
//              }
//            }
          };        
        }

        @Override
        public void close() throws IOException {
          if ( openCount.decrementAndGet() == 0 ) {
            if ( closed ) {
              return;
            } else {
              closed = true;
            }
            free(data[0]);
            removeChunk(this);
          }
        }
//  ONLY FOR DEBUGGING LEAKS
//        @Override
//        protected void finalize() throws Throwable {
//          super.finalize(); 
//          if ( !closed ) {
//            LOGGER.warn("LEAK DETECTED");
//            close();
//          }
//        }
      }

      private class VirtualChunk implements Chunk, Closeable, Loadable {
        
        private final long offset;
        private long length = Long.MAX_VALUE;
        private long position = 0;
        private boolean loaded = false;
        private ByteBuffer cache;
        private AtomicInteger loadedouts;
        private boolean closed = false;

        public VirtualChunk(long offset) throws IOException {
            this.offset = offset;
            register();
        }
        
        @Override
        public void load() throws IOException {
          if ( !loaded ) {
            free(cache);
            cache = readDirect(this.offset, allocate((int)length));
            cache.position((int)position);
            loaded = true;
            loadedouts = new AtomicInteger(1);
          }
        }
        
        private void register() throws IOException {
          addChunk(this);
        }
        
        private ByteBuffer cache(int size) {
          if ( cache == null ) {
            cache = allocate(32);
          }
          if ( size < cache.capacity() ) {
              cache.clear().limit(size);
              return cache;
          }
          return ByteBuffer.allocate(size);
        }
        
        public void setLength(long length) {
            this.length = length;
        }

        @Override
        public void close() throws IOException {
          if ( closed ) {
            return;
          } else {
            closed = true;
          }
          if ( !loaded || loadedouts.decrementAndGet() == 0 ) {
            free(cache);
          }
          removeChunk(this);
        }

        @Override
        public ByteBuffer[] getBuffers() {
          if ( loaded ) {
            cache.position((int)position).limit((int)length);
            return new ByteBuffer[] {cache};
          }
            return null;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public long remaining() {
            return length - position;
        }

        @Override
        public void limit(long v) {

        }

        @Override
        public boolean hasRemaining() {
            return remaining() > 0 ;
        }

        @Override
        public byte get(long pos) {
          if ( loaded ) {
            return cache.get((int)pos);
          }
          ByteBuffer buffer = readVirtualDirect(offset + pos, cache(Byte.SIZE/Byte.SIZE));
          return buffer.get();
        }

        @Override
        public short getShort(long pos) {
          if ( loaded ) {
            return cache.getShort((int)pos);
          }
          ByteBuffer buffer = readVirtualDirect(offset + pos, cache(Short.SIZE/Byte.SIZE));
          return buffer.getShort();
        }

        @Override
        public int getInt(long pos) {
          if ( loaded ) {
            return cache.getInt((int)pos);
          }
          ByteBuffer buffer = readVirtualDirect(offset + pos, cache(Integer.SIZE/Byte.SIZE));
          return buffer.getInt();
        }

        @Override
        public long getLong(long pos) {
          if ( loaded ) {
            return cache.getLong((int)pos);
          }
          ByteBuffer buffer = readVirtualDirect(offset + pos, cache(Long.SIZE/Byte.SIZE));
          return buffer.getLong();
        }

        @Override
        public byte get() {
            return get(position++ + offset);
        }

        @Override
        public void put(byte v) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public byte peek() {
            return get(position + offset);
        }

        @Override
        public long getLong() {
            try {
                return getLong(position + offset);
            } finally {
                position  += Long.SIZE/Byte.SIZE;
            }
        }

        @Override
        public void putLong(long v) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public long peekLong() {
            return getLong(position + offset);
        }

        @Override
        public short getShort() {
            try {
                return getShort(position + offset);
            } finally {
                position  += Short.SIZE/Byte.SIZE;
            }
        }

        @Override
        public void putShort(short v) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public short peekShort() {
            return getShort(position + offset);
        }

        @Override
        public int getInt() {
            try {
                return getInt(position + offset);
            } finally {
                position  += Integer.SIZE/Byte.SIZE;
            }
        }

        @Override
        public void putInt(int v) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public int peekInt() {
            return getInt(position + offset);
        }

        @Override
        public int get(byte[] buf) {
            int read = 0;
            try {
              if ( loaded ) {
                cache.position((int)position);
                cache.get(buf);
              } else {
                ByteBuffer wrapped = ByteBuffer.wrap(buf);
                if ( wrapped.capacity() > this.length - this.position ) {
                  wrapped.limit((int)(length - position));
                }
                read = readVirtualDirect(offset + position, wrapped).remaining();
              }
            } finally {
                position  += read;
            }
            return read;
        }

        @Override
        public int put(byte[] buf) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public void skip(long jump) {
            if ( position + jump > length ) {
                throw new RuntimeException("pos:" + position + " len:" + length + " mov:" + jump);
            }
            position += jump;
        }

        @Override
        public ByteBuffer[] getBuffers(long size) {
            if ( position + size > length ) {
                throw new RuntimeException("pos:" + position + " len:" + length + " mov:" + size);
            }
            try {
              if ( size == 0 ) {
                return EMPTY;
              } else {
                if ( loaded ) {
                  cache.position((int)(position)).limit((int)(position + size));
                  return new ByteBuffer[] {cache.slice()};  //  not safe on close
                }
                return new ByteBuffer[] {readVirtualDirect(offset + position,allocate((int)size))};
              }
            } finally {
                position += size;
            }
        }

        @Override
        public Chunk getChunk(long size) {
            try {
              if ( position + size > length ) {
                  throw new RuntimeException("pos:" + position + " len:" + length + " mov:" + size);
              }
              if ( size == 0 ) {
                return new WrappingChunk(EMPTY);
              } else {
                if ( loaded ) {
                    loadedouts.incrementAndGet();
                    cache.position((int)(position)).limit((int)(position + size));
                    final ByteBuffer[] base = new ByteBuffer[] {cache.slice()};
                    
                    return new CloseableChunk() {
                      
                      boolean localclosed = false;
                      
                      @Override
                      public ByteBuffer[] getBuffers() {
                        return base;
                      }

                      @Override
                      public void close() throws IOException {
                        if ( localclosed ) {
                          return;
                        }
                        localclosed = true;
                        if ( loadedouts.decrementAndGet() == 0 && closed ) {
                          free(cache);
                        }
                      }
//  ONLY FOR DEBUGGING LEAKS
//                      @Override
//                      protected void finalize() throws Throwable {
//                        super.finalize(); 
//                        if ( !localclosed ) {
//                          LOGGER.warn("LEAK DETECTED");
//                          close();
//                        }
//                      }
                    };
                } else {
                  return new FullChunk(offset + position, size);
                }
              }
            } catch ( IOException ioe ) {
              throw new RuntimeException(ioe);
            } finally {
                position += size;
            }
        }

        @Override
        public void flip() {
        }

        @Override
        public void clear() {
          position = 0;
        }

        @Override
        public String toString() {
          return "VirtualChunk{" + "offset=" + offset + ", length=" + length + ", position=" + position + ", closed=" + closed + '}';
        }
    }
    
    private static abstract class CloseableChunk extends AbstractChunk implements Closeable {
      
    }
}
