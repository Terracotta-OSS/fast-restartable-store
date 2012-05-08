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

    static final int FILE_HEADER_SIZE = 42;
    private static final String LOCKED_FILE_ACCESS = "could not obtain file lock";
    private static final short IMPL_NUMBER = 02;
    private final NIOStreamImpl parent;
    private final int segNum;
    private final File src;
    private FileChannel segment;
//  for reading and writing
    private FileBuffer buffer;
//  for reading 
    private ReadbackStrategy strategy;
//  for writing
    private FileLock lock;
    private List<Long> writeJumpList;
    private long lowestMarker;
    private long minMarker;
    private long maxMarker;
    private UUID streamId;

    NIOSegmentImpl(NIOStreamImpl p, File file) {
        this.parent = p;
        this.src = file;
        this.segNum = NIOSegmentList.convertSegmentNumber(file);
    }

    File getFile() {
        return src;
    }

    NIOSegmentImpl openForReading(BufferSource reader) throws IOException {
        long fileSize = 0;

        segment = new FileInputStream(src).getChannel();

        fileSize = segment.size();

        if (fileSize < FILE_HEADER_SIZE) {
            throw new IOException("bad header");
        }
        
//        int bufferSize = 1024;
        int bufferSize = (fileSize > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) fileSize;

        ByteBuffer fbuf = reader.getBuffer(bufferSize);
        if (fbuf == null) {
            System.out.println("WARNING: direct memory unavailable. Allocating on heap.  Fix configuration for more direct memory.");
            fbuf = ByteBuffer.allocate(1024 * 1024);
        }

        buffer = new FileBuffer(segment, fbuf);

        buffer.partition(FILE_HEADER_SIZE).read(1);
        readFileHeader(buffer);

        if (buffer.capacity() >= segment.size()) {
//  the buffer is big enough for the whole file.  cheat by reading forward 
//  then queueing backward.
           strategy = new WholeFileReadbackStrategy(buffer);
        } else {
            strategy = new MappedReadbackStrategy(new FileInputStream(src).getChannel());
        }

        return this;
    }

    private void readFileHeader(Chunk readBuffer) throws IOException {
        byte[] code = new byte[4];
        readBuffer.get(code);
        if (!SegmentHeaders.LOG_FILE.validate(code)) {
            throw new IOException("file header is corrupted " + new String(code));
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
        lowestMarker = readBuffer.getLong();
        minMarker = readBuffer.getLong();
    }

    //  open and write the header.
    NIOSegmentImpl openForWriting(BufferSource pool) throws IOException {
        if (src.length() > 0) {
            throw new IOException("bad access");
        }

        segment = new FileOutputStream(src).getChannel();
        lock = segment.tryLock();
        if (lock == null) {
            throw new IOException(LOCKED_FILE_ACCESS);
        }
        
        int request = 512 * 1024;
        ByteBuffer bb = null;
        while ( bb == null ) {
            if ( request < 512 * 1024 ) bb = ByteBuffer.allocate(request);
            else bb = pool.getBuffer(request/=2);
        }

        buffer = new FileBuffer(segment, bb);

        writeJumpList = new ArrayList<Long>();

        return this;
    }

    void insertFileHeader(long lowestMarker, long marker) throws IOException {
        this.streamId = parent.getStreamId();

        buffer.clear();

        buffer.put(SegmentHeaders.LOG_FILE.getBytes());
        buffer.putShort(IMPL_NUMBER);
        buffer.putInt(segNum);
        buffer.putLong(streamId.getMostSignificantBits());
        buffer.putLong(streamId.getLeastSignificantBits());
        buffer.putLong(lowestMarker);
        buffer.putLong(marker);

        buffer.write(1);
    }
    
    private long piggybackBufferOptimization(ByteBuffer used) throws IOException {
        long amt = used.remaining();
        int estart = used.limit();
        int position = used.position() - (ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
//  expand buffer
        used.position(position);
        used.limit(estart + (2*ByteBufferUtils.LONG_SIZE) + ByteBufferUtils.INT_SIZE);
//  place header            
        used.putInt(position,SegmentHeaders.CHUNK_START.getIntValue());
        used.putLong(position + ByteBufferUtils.INT_SIZE,amt);
//  place footer          
        used.putLong(estart,amt);
        used.putLong(estart + ByteBufferUtils.LONG_SIZE,maxMarker);
        used.putInt(estart + (2*ByteBufferUtils.LONG_SIZE),SegmentHeaders.FILE_CHUNK.getIntValue());
//   write it all out
        amt = 0;
        while ( used.hasRemaining() ) {
            amt += segment.write(used);
        }
        writeJumpList.add(segment.position());
        return amt;
    }

    public long append(Chunk c, long maxMarker) throws IOException {
        int writeCount = 0;
        buffer.clear();
        this.maxMarker = maxMarker;
        if (
//  very specfic optimization to write out buffers as quickly as possible by using extra space in 
//    passed in buffer creating one large write rather than small header writes
            c.getBuffers().length == 1 && !c.getBuffers()[0].isReadOnly() && c.getBuffers()[0].isDirect() &&
            c.getBuffers()[0].position() > ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE &&
            c.getBuffers()[0].capacity() - c.getBuffers()[0].limit() > (2*ByteBufferUtils.LONG_SIZE) + ByteBufferUtils.INT_SIZE
        ) {
            return piggybackBufferOptimization(c.getBuffers()[0]);
        } else {
            buffer.clear();
            buffer.partition(ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
            long amt = c.remaining();
            assert (amt == c.length());
            buffer.put(SegmentHeaders.CHUNK_START.getBytes());
            buffer.putLong(amt);
            buffer.insert(c.getBuffers(), 1);
            buffer.putLong(amt);
            buffer.putLong(maxMarker);
            buffer.put(SegmentHeaders.FILE_CHUNK.getBytes());
            writeCount = c.getBuffers().length + 2;
            try {
                return buffer.write(writeCount);
            } finally {
                writeJumpList.add(buffer.offset());
            }
        }

    }

    public long close() throws IOException {
        if (segment == null || !segment.isOpen()) {
            return 0;
        }

        if (lock != null) {
            buffer.clear();
            buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
            writeJumpList(buffer);
            buffer.write(1);
            //  TODO: is this force neccessary?  not sure, research
            segment.force(false);
            lock.release();
            segment.close();
            buffer = null;
            lock = null;
        } else {
        }

        segment = null;

        return (buffer != null) ? buffer.getTotal() : 0;
    }
    
    private void writeJumpList(FileBuffer target) throws IOException {
        target.clear();
        target.put(SegmentHeaders.CLOSE_FILE.getBytes());
        for (long jump : writeJumpList) {
            if (target.remaining() < ByteBufferUtils.LONG_SIZE
                    + ByteBufferUtils.SHORT_SIZE
                    + ByteBufferUtils.INT_SIZE) {
                target.write(1);
                target.clear();
            }
            target.putLong(jump);
        }
        if (writeJumpList.size() < Short.MAX_VALUE) {
            target.putShort((short) writeJumpList.size());
        } else {
            target.putShort((short) -1);
        }
        target.put(SegmentHeaders.JUMP_LIST.getBytes());
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

    long getBaseMaker() {
        return minMarker;
    }

    long getMinimumMarker() {
        return lowestMarker;
    }

    long getMaximumMarker() {
        return maxMarker;
    }

    public boolean isClosed() {
        return (segment == null);
    }

    public boolean wasProperlyClosed() throws IOException {
        if (strategy != null && strategy.isConsistent()) {
//  close file magic was seen as part of read back scan
            return true;
        } else {
// do it the hard way
            if (segment.size() < FILE_HEADER_SIZE + ByteBufferUtils.INT_SIZE) {
                return false;
            }
            buffer.clear();
            buffer.position(segment.size() - buffer.capacity()).read(1);
            int fileEnd = buffer.getInt(buffer.remaining() - ByteBufferUtils.INT_SIZE);
            if (SegmentHeaders.CLOSE_FILE.validate(fileEnd)) {
                return true;
            }
            if (SegmentHeaders.JUMP_LIST.validate(fileEnd)) {
                return true;
            }

            return false;
        }
    }

    public Chunk next(Direction dir) throws IOException {
        if (strategy.hasMore(dir)) {
            return strategy.iterate(dir);
        }
        throw new IOException("segment bounds");
    }

    public boolean hasMore(Direction dir) throws IOException {
        return strategy.hasMore(dir);
    }

    public long length() throws IOException {
        return segment.size();
    }

    public long position() throws IOException {
        return segment.position();
    }
    
    public boolean isEmpty() {
        return writeJumpList.isEmpty();
    }

    public void limit(long pos) throws IOException {
        buffer.clear();
        buffer.position(pos - ByteBufferUtils.INT_SIZE);
        buffer.partition(4);
        buffer.read(1);
        byte[] code = new byte[4];
        buffer.get(code);
        if (!SegmentHeaders.FILE_CHUNK.validate(code)) {
            throw new IOException("bad truncation " + new String(code));
        }
        
        FileChannel fc = new FileOutputStream(src,true).getChannel();
        fc.truncate(pos);
        fc.position(pos);
        ByteBuffer close = ByteBuffer.allocate(4096);
        FileBuffer target = new FileBuffer(fc,close);
        target.put(SegmentHeaders.CLOSE_FILE.getBytes());
        writeJumpList(target);
        target.write(1);
        fc.force(true);
        fc.close();
    }

    boolean last() throws IOException {
        IntegrityReadbackStrategy find = new IntegrityReadbackStrategy(buffer);
        int count = 0;
        try {
            while (find.hasMore(Direction.FORWARD)) {
                try {
                    find.iterate(Direction.FORWARD);
                    count+=1;
                } catch (IOException ioe) {
                    return false;
                }
            }
        } finally {
            buffer.clear();
            maxMarker = find.getLastValidMarker();           
            buffer.position(find.getLastValidPosition());
            writeJumpList = find.getJumpList();
        }
        return find.wasClosed();
    }
}
