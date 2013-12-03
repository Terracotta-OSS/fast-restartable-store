/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.AbstractChunk;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.Loadable;
import com.terracottatech.frs.io.WrappingChunk;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public abstract class BaseBufferReadbackStrategy extends AbstractReadbackStrategy implements Closeable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ReadbackStrategy.class);
  protected static final ByteBuffer[] EMPTY = new ByteBuffer[]{};
  private final FileChannel channel;
  private final BufferSource source;
  private final AtomicInteger outchunks = new AtomicInteger();
  private volatile boolean closeRequested = false;

  public BaseBufferReadbackStrategy(Direction dir, FileChannel channel, BufferSource source) throws IOException {
        this.channel = channel;
        this.source = source;
  }

  protected void addChunk(Chunk c) throws IOException {
    if (!this.channel.isOpen()) {
      throw new IOException("file closed");
    }
    outchunks.incrementAndGet();
  }
  
  protected FileChannel getChannel() {
    return channel;
  }

  protected void removeChunk(Chunk c) throws IOException {
    if (outchunks.decrementAndGet() == 0 && closeRequested) {
      this.channel.close();
    }
  }

  protected ByteBuffer readFully(int amount, ByteBuffer get) throws IOException {
    get.mark();
    get.limit(get.position() + amount);
    while (get.hasRemaining()) {
      channel.read(get);
    }
    get.reset();
    return get;
  }

  protected ByteBuffer readDirect(long position, ByteBuffer get) throws IOException {
    int read = 0;
    get.mark();
    while (get.hasRemaining()) {
      int amt = channel.read(get, position + read);
      if (amt < 0) {
        throw new EOFException();
      } else {
        read += amt;
      }
    }
    get.reset();
    return get;
  }

  protected int writeDirect(long position, ByteBuffer get) throws IOException {
    throw new UnsupportedOperationException("read only");
  }

  protected ByteBuffer readVirtualDirect(long positon, ByteBuffer get) {
    try {
      return readDirect(positon, get);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  protected ByteBuffer allocate(int size) {
    if (size == 0) {
      throw new AssertionError();
    }
    if (source == null) {
      return ByteBuffer.allocate(size);
    }
    return source.getBuffer(size);
    //  FOR DEBUGGING ONLY
//          ByteBuffer bigger = source.getBuffer(size + 4);
//          if ( bigger.getInt(0) != 0 ) {
//            throw new AssertionError("stomp " + Integer.toHexString(bigger.getInt(0)));
//          }
//          bigger.putInt(0xEAC0CA11);
//          return bigger;
  }

  protected void free(ByteBuffer buf) {
    if (source != null && buf != null) {
//        FOR DEBUGGING ONLY
//              if ( buf.getInt(0) != 0xEAC0CA11 ) {
//                throw new AssertionError("stomp " + Integer.toHexString(buf.getInt(0)));
//              }
//              buf.clear();
//              while ( buf.hasRemaining() ) {
//                buf.put((byte)0);
//              }
      source.returnBuffer(buf);
    }
  }

  protected int writeVirtualDirect(long positon, ByteBuffer put) {
    try {
      return writeDirect(positon, put);
    } catch (IOException ioe) {
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
    if (outchunks.get() == 0) {
      this.channel.close();
    }
  }
  
        
  protected class Marker {
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
          if ( lhint == 0 ) {
            return new VirtualChunk(start);
          } else {
            return new VirtualChunk(start+12,lhint-12);
          }
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
          return new CloseableChunk() {
            volatile boolean closed = false;

            @Override
            public ByteBuffer[] getBuffers() {
              return sub;
            }

            @Override
            public void close() throws IOException {
              closed = true;
              parent.close();
            }
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
            data[0] = null;
            removeChunk(this);
          }
        }
      }

      protected class VirtualChunk implements Chunk, Closeable, Loadable {
        
        private final long offset;
        private long length = Long.MAX_VALUE;
        private long position = 0;
        private boolean loaded = false;
        private ByteBuffer cache;
        private AtomicInteger loadedouts;
        private volatile boolean closed = false;

        public VirtualChunk(long offset) throws IOException {
            this.offset = offset;
            register();
            findLength();
        }
        
        public VirtualChunk(long offset, long len) throws IOException {
            this.offset = offset;
            this.length = len;
            this.position = 12;
//            findLength();
//            if ( len != this.length ) {
//              throw new AssertionError();
//            }
            register();
        }        
        
        private void findLength() throws IOException {
          Chunk header = this.getChunk(12);
          try {
            int cs = header.getInt(); 
            if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
              throw new AssertionError("not valid");
            }
            length = header.getLong() + 12;
            if ( length > Integer.MAX_VALUE ) {
                throw new IOException("buffer overflow");
            }
          } finally {
            if ( header instanceof Closeable ) {
                ((Closeable)header).close();
            }
          }
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
            return get(position++);
        }

        @Override
        public void put(byte v) {
            throw new UnsupportedOperationException("read only"); 
        }

        @Override
        public byte peek() {
            return get(position);
        }

        @Override
        public long getLong() {
            try {
                return getLong(position);
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
            return getLong(position);
        }

        @Override
        public short getShort() {
            try {
                return getShort(position);
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
            return getShort(position);
        }

        @Override
        public int getInt() {
            try {
                return getInt(position);
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
            return getInt(position);
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
                  try {
                    cache.position((int)(position)).limit((int)(position + size));
                    return new ByteBuffer[] {cache.slice()};  //  not safe on close
                  } finally {
                    cache.clear();
                  }
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
                    cache.clear();
                    
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
