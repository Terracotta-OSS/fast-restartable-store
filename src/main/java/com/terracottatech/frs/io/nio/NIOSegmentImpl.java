/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

/**
 *
 * @author mscott
 */
class NIOSegmentImpl {
  static final int            FILE_HEADER_SIZE = 26;
  private static final String LOCKED_FILE_ACCESS = "could not obtain file lock";
  private static final short  IMPL_NUMBER = 02;

    private final NIOStreamImpl parent;
    private final int           segNum;
    private final File          src;
    private FileChannel         segment;
    
//  for reading 
    private ReadbackStrategy    strategy;

//  for writing
    private FileLock            lock;
    private FileBuffer          buffer;
    private ArrayList<Long>     writeJumpList;
    

    
    private UUID streamId;
        
    NIOSegmentImpl(NIOStreamImpl p, File file) {
        this.parent = p;
        this.src = file;
        this.segNum = p.convertSegmentNumber(file);
    }
    
    File getFile() {
        return src;
    }

    NIOSegmentImpl openForReading(BufferSource reader) throws IOException {
        long fileSize = 0;
        
        segment = new FileInputStream(src).getChannel();
            
        fileSize = segment.size();
        
        if ( fileSize < FILE_HEADER_SIZE ) throw new IOException("bad header");
        
        int bufferSize = ( fileSize > Integer.MAX_VALUE ) ? Integer.MAX_VALUE : (int)fileSize;
        
        ByteBuffer fbuf = reader.getBuffer(bufferSize);
        while ( fbuf == null ) {
            if ( bufferSize < 100 * 1024 ) throw new IOException("no buffer available");
            fbuf = reader.getBuffer(bufferSize/=2);
        }
        
        if ( fbuf == null ) {
            MappedReadbackStrategy mapped = new MappedReadbackStrategy(segment);
            
            strategy = mapped;
            
            readFileHeader(mapped.getBuffer());
        } else {
            buffer = new FileBuffer(segment, fbuf);
            
            buffer.partition(FILE_HEADER_SIZE).read(1);
            readFileHeader(buffer);

            if ( bufferSize >= segment.size() ) {
    //  the buffer is big enough for the whole file.  cheat by reading forward 
    //  then queueing backward.
                strategy = new WholeFileReadbackStrategy(buffer);
            } else {
                throw new UnsupportedOperationException("only whole file read back supported");
            }
        }
                 
        return this;
   }
    
   private void readFileHeader(Chunk readBuffer) throws IOException {        
        int lfm = readBuffer.getInt();
        if (!SegmentHeaders.LOG_FILE.validate(lfm)) {
            throw new IOException("file header is corrupted");
        }
        short impl = readBuffer.getShort();
        int checkSeg = readBuffer.getInt();
        if (segNum != checkSeg) {
            throw new IOException("the filename does not match the internal file structure");
        }

        if (impl != IMPL_NUMBER) {
            throw new IOException("unknown implementation number");
        }
        
        streamId = new UUID(readBuffer.getLong(), readBuffer.getLong());
   }

    //  open and write the header.
    NIOSegmentImpl openForWriting(BufferSource pool) throws IOException {
        if ( src.length() > 0 ) {
            throw new IOException("bad access");
        }
        
        segment = new FileOutputStream(src).getChannel();
        lock = segment.tryLock();
        if ( lock == null ) throw new IOException(LOCKED_FILE_ACCESS);
        pool.reclaim();
        
        buffer = new FileBuffer(segment, pool.getBuffer(100 * 1024));
        
        writeJumpList = new ArrayList<Long>();
        
        insertFileHeader();

        return this;
    }
    
    private void insertFileHeader() throws IOException {
        this.streamId = parent.getStreamId();
        
        buffer.clear();

        buffer.put(SegmentHeaders.LOG_FILE.getBytes());
        buffer.putShort(IMPL_NUMBER);
        buffer.putInt(segNum);
        buffer.putLong(streamId.getMostSignificantBits());
        buffer.putLong(streamId.getLeastSignificantBits());
        buffer.write(1);
    }

    // getBuffers does the bulk of the work defining header for LogRecord
    public long append(Chunk c) throws IOException {

        buffer.clear();
        buffer.partition(ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
        long amt = c.remaining();
        assert(amt == c.length());
        buffer.put(SegmentHeaders.CHUNK_START.getBytes());
        buffer.putLong(amt);
        buffer.insert(c.getBuffers(), 1);
        buffer.putLong(amt);
        buffer.put(SegmentHeaders.FILE_CHUNK.getBytes());
        long wl = buffer.write(c.getBuffers().length + 2);
        writeJumpList.add(buffer.offset());

        return wl;
    }

    public long close() throws IOException {
        if (segment == null || !segment.isOpen()) {
            return 0;
        }
        
        if ( lock != null ) {
            buffer.clear();
            buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
            for (long jump : writeJumpList ) {
                if ( buffer.remaining() < ByteBufferUtils.LONG_SIZE + 
                       ByteBufferUtils.SHORT_SIZE +
                       ByteBufferUtils.INT_SIZE
                ) {
                    buffer.write(1);
                    buffer.clear();
                }
                buffer.putLong(jump);
            }
            if ( writeJumpList.size() < Short.MAX_VALUE ) {
                buffer.putShort((short)writeJumpList.size());
            } else {
                buffer.putShort((short)-1);
            }
            buffer.put(SegmentHeaders.JUMP_LIST.getBytes());
            buffer.write(1);
            //  TODO: is this force neccessary?  not sure, research
            segment.force(false);
            lock.release();
            segment.close();
            lock = null;
        } else {
            
        }
        
        segment = null;
        
        return (buffer != null ) ? buffer.getTotal() : 0;
    }
//  assume single threaded
    public long fsync() throws IOException {
        long pos = segment.position();
        segment.force(false);
        return pos;
    }
    
    public int getSegmentId() {
        return segNum;
    }
    
    UUID getStreamId() {
        return streamId;
    }

    public boolean isClosed() {
        return (segment == null);
    }

    public Chunk next(Direction dir) throws IOException {
        if ( strategy.hasMore(dir) ) return strategy.iterate(dir);
        throw new IndexOutOfBoundsException();
    }
    
    public boolean hasMore(Direction dir) throws IOException {
        return strategy.hasMore(dir);
    }
    
    public long length() throws IOException {
        return segment.size();
    }
    
    public void limit(long pos) throws IOException {
        segment.truncate(pos);
    }
    
}
