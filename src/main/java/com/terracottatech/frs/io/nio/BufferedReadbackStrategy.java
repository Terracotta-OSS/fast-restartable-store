/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import java.io.Closeable;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 *
 * @author mscott
 */
class BufferedReadbackStrategy extends AbstractReadbackStrategy implements Closeable {

    private final   FileBuffer buffer;
    private final   NavigableMap<Long,Marker>              boundaries;
    private volatile boolean sealed;
    private long offset = 0;
    private long length = 0;    
        
    public BufferedReadbackStrategy(Direction dir, FileBuffer buffer) throws IOException {
        this.buffer = buffer;
        boundaries = new TreeMap<Long,Marker>();
        length = buffer.position();
        sealed = createIndex();
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
            updateIndex();
            return false;
        } else {
            for ( Long next : jumps ) {
                try {
                  data.position(next.intValue() - 20);
                  data.partition(16);
                  data.read(1);
                  long clen = data.getLong();
                  long marker = data.getLong();
                  boundaries.put(marker,new Marker(next - 20 - clen - 12, marker));
                } catch ( Throwable t ) {
                    throw new AssertionError(t);
                }
            }
            length = data.size();
            return true;
        }
//        data.skip(NIOSegment.FILE_HEADER_SIZE);
    }   
    
    private FileBuffer getFileBuffer() {
//   insure under lock
        if ( !Thread.holdsLock(this) ) {
            throw new AssertionError("not holding lock");
        }
        buffer.clear();
        return this.buffer;
    }

    @Override
    public long getMaximumMarker() {
        try {
            return boundaries.lastKey();
        } catch ( NoSuchElementException no ) {
            return Long.MIN_VALUE;
        }
    }
    
    
    private synchronized void updateIndex() throws IOException {
        FileBuffer data = getFileBuffer();
        if ( length + 4 > data.size() ) {
            return;
        } else {
            data.position(length);
        }
        long start = data.position();
        data.partition(4,8);
        try {
            data.read(1);
        } catch ( IOException ioe ) {
            System.out.println("bad length " + length + " " + data.position() + " " + data.size());
            throw ioe;
        }
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
      Long key = null;
      try {
        Map.Entry<Long,Marker> e = ( dir == Direction.FORWARD ) ? boundaries.ceilingEntry(offset) : boundaries.floorEntry(offset);
        if (e == null ) {
            return null;
        }
        key = e.getKey();
        return e.getValue().getChunk().copyDetachedChunk();
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
    
    private ByteBuffer readDirect(long position, ByteBuffer get) throws IOException {
        FileChannel src = buffer.getFileChannel();
        int read = 0;
        while ( get.hasRemaining() ) {
            read += src.read(get,position + read);
        }
        get.flip(); 
        return get;
    }      
    
    private synchronized int writeDirect(long position, ByteBuffer get) throws IOException {
        throw new UnsupportedOperationException("read only");
    }    
    
    private ByteBuffer readVirtualDirect(long positon, ByteBuffer get) {
        try {
            return readDirect(positon, get);
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
    }
    
    private int writeVirtualDirect(long positon, ByteBuffer put) {
        try {
            return writeDirect(positon, put);
        } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
        }
    }    
    
    @Override
    public synchronized long size() throws IOException {
        return this.buffer.size();
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.buffer.close();
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
      
      public VirtualChunk getChunk() throws IOException {
          VirtualChunk c = new VirtualChunk(start);
          Chunk header = c.getChunk(12);
            int cs = header.getInt(); 
            if ( !SegmentHeaders.CHUNK_START.validate(cs) ) {
              throw new AssertionError("not valid");
            }
            long len = header.getLong();
            if ( len > Integer.MAX_VALUE ) {
                throw new IOException("buffer overflow");
            }
            if ( header instanceof Closeable ) {
                ((Closeable)header).close();
            }
            c.setLength(len);
          return c;
      }
    }
    
    private class FullChunk extends AbstractChunk {
        
        private final ByteBuffer[] data;

        public FullChunk(long offset, long length) {
            data = new ByteBuffer[] {readVirtualDirect(offset, ByteBuffer.allocate((int)length))};
        }

        @Override
        public ByteBuffer[] getBuffers() {
            return data;
        }
        
    }   
    
    private class VirtualChunk implements Chunk, Closeable {
        
        private final long offset;
        private long length;
        private long position = 0;
        private final ByteBuffer cache;

        public VirtualChunk(long offset) {
            this.offset = offset;
            cache = (buffer.getBufferSource() != null) ? buffer.getBufferSource().getBuffer(32) : null;
        }
        
        public VirtualChunk(long offset, long length) {
            this.offset = offset;
            this.length = length;
            cache = (buffer.getBufferSource() != null) ? buffer.getBufferSource().getBuffer(32) : null;
        }
        
        public FullChunk copyDetachedChunk() {
            return new FullChunk(offset + position,length);
        }
        
        private ByteBuffer allocate(int size) {
            if ( cache != null && size < cache.capacity() ) {
                cache.clear().limit(size);
                return cache;
            } else if ( buffer.getBufferSource() != null ) {
                return buffer.getBufferSource().getBuffer(size);
            }
            return ByteBuffer.allocate(size);
        }
        
        public void setLength(long length) {
            this.length = length;
        }

        @Override
        public void close() throws IOException {
            if ( buffer.getBufferSource() != null && cache != null ) {
                buffer.getBufferSource().returnBuffer(cache);
            }
        }

        @Override
        public ByteBuffer[] getBuffers() {
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
            ByteBuffer buffer = readVirtualDirect(pos, allocate(Byte.SIZE/Byte.SIZE));
            return buffer.get();
        }

        @Override
        public short getShort(long pos) {
            ByteBuffer buffer = readVirtualDirect(pos, allocate(Short.SIZE/Byte.SIZE));
            return buffer.getShort();
        }

        @Override
        public int getInt(long pos) {
            ByteBuffer buffer = readVirtualDirect(pos, allocate(Integer.SIZE/Byte.SIZE));
            return buffer.getInt();
        }

        @Override
        public long getLong(long pos) {
            ByteBuffer buffer = readVirtualDirect(pos, allocate(Long.SIZE/Byte.SIZE));
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
                read = readVirtualDirect(offset + position, ByteBuffer.wrap(buf)).remaining();
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
                throw new BufferOverflowException();
            }
            position += jump;
        }

        @Override
        public ByteBuffer[] getBuffers(long length) {
            try {
                return new ByteBuffer[] {readVirtualDirect(offset + position,ByteBuffer.allocate((int)length))};
            } finally {
                position += length;
            }
        }

        @Override
        public Chunk getChunk(long length) {
            try {
                return new VirtualChunk(offset + position, length);
            } finally {
                position += length;
            }
        }

        @Override
        public void flip() {

        }

        @Override
        public void clear() {

        }
        
    }
}
