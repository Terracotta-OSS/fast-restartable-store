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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

/**
 *
 * @author mscott
 */
class NIOSegmentImpl {

    private final NIOStreamImpl parent;
    private final int           segNum;
    private final Direction     direction;

    private final File          src;
    private FileLock            lock;
    private FileChannel         segment;
    private FileBuffer          buffer;
    private ReadbackStrategy    strategy;
    private ArrayList<Long>     writeJumpList;
    
    private long                limit = 10 * 1024 * 1024;
    static final int    FILE_HEADER_SIZE = 26;
    private static final String LOCKED_FILE_ACCESS = "could not obtain file lock";
    private static final short  IMPL_NUMBER = 02;
    
    private UUID streamid;
    
    NIOSegmentImpl(NIOStreamImpl p, Direction dir, File file, long segSize) {
        this.parent = p;
        this.src = file;
        this.limit = segSize;
        this.direction = dir;
        this.segNum = p.convertSegmentNumber(file);
    }
    
    File getFile() {
        return src;
    }

    NIOSegmentImpl openForReading(BufferSource reader) throws IOException {
        segment = new FileInputStream(src).getChannel();
        this.limit = segment.size();
        
        if ( this.limit < FILE_HEADER_SIZE ) throw new IOException("bad header");
        
        int size = ( this.limit > Integer.MAX_VALUE ) ? Integer.MAX_VALUE : (int)this.limit;
        
        ByteBuffer fbuf = reader.getBuffer(size);
        while ( fbuf == null ) {
            if ( size < 100 * 1024 ) throw new IOException("no buffer available");
            fbuf = reader.getBuffer(size/=2);
        }
        
        if ( fbuf == null ) {
            MappedReadbackStrategy mapped = new MappedReadbackStrategy(segment);
            
            strategy = mapped;
            
            readFileHeader(mapped.getBuffer());
        } else {
            buffer = new FileBuffer(segment,fbuf);
            
            buffer.partition(FILE_HEADER_SIZE).read(1);
            readFileHeader(buffer);

            if ( size >= this.limit ) {
    //  the buffer is big enough for the whole file.  cheat by reading forward 
    //  then queueing backward.
                strategy = new WholeFileReadbackStrategy(buffer);
            } else {
                if ( direction == Direction.REVERSE ) {
//                    strategy = new ReverseReadbackStrategy(buffer);
                    throw new UnsupportedOperationException("only whole file read back supported");
                } else {
                    throw new UnsupportedOperationException("only backward iteration currently");
                }
//                buffer.clear();
            }
        }
        
        strategy.queue(direction);
         
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
        
        streamid = new UUID(readBuffer.getLong(), readBuffer.getLong());   
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
        this.streamid = parent.getStreamId();
        
        buffer.clear();

        buffer.put(SegmentHeaders.LOG_FILE.getBytes());
        buffer.putShort(IMPL_NUMBER);
        buffer.putInt(segNum);
        buffer.putLong(streamid.getMostSignificantBits());
        buffer.putLong(streamid.getLeastSignificantBits());
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
        writeJumpList.add(buffer.getTotal());
        if (segment.position() >= limit) {
            close();
        }
        return wl;
    }

    public void close() throws IOException {
        if (segment == null || !segment.isOpen()) {
            return;
        }
        if ( lock != null ) {
            buffer.clear();
            buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
            for (long jump : writeJumpList ) {
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
        return streamid;
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
        if ( lock != null ) return segment.size();
        return limit;
    }

    public long remains() throws IOException {
        return limit - segment.position();
    }
    
    public void limit(long pos) throws IOException {
        segment.truncate(pos);
        limit = segment.size();
    }
    
}
