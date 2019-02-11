/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

/**
 *
 * @author mscott
 */
class WritingSegment extends NIOSegment implements Iterable<Chunk>, Closeable {

    private FileBuffer buffer;
    private static final short IMPL_NUMBER = 02;
    private long maxMarker;
    private WritingSegmentJumpList writeJumpList;
    private long totalWrite;
    private boolean existingFile = false;

    WritingSegment(NIOStreamImpl p, File file) {
        super(p, file);
        if ( file.exists() ) {
            existingFile = true;
        } 
    }

    long getMaximumMarker() {
        return maxMarker;
    }
    
    long getTotalWritten() {
        return totalWrite;
    }

    @Override
    void insertFileHeader(long lowestMarker, long marker) throws IOException {
        super.insertFileHeader(lowestMarker, marker);

        buffer.clear();

        buffer.put(SegmentHeaders.LOG_FILE.getBytes());
        buffer.putShort(IMPL_NUMBER);
        buffer.putInt(super.getSegmentId());
        buffer.putLong(super.getStreamId().getMostSignificantBits());
        buffer.putLong(super.getStreamId().getLeastSignificantBits());
        buffer.putLong(super.getMinimumMarker());
        buffer.putLong(super.getBaseMarker());

        buffer.write(1);
    }

    //  open and write the header.
    synchronized WritingSegment open() throws IOException, HeaderException {
        while (buffer == null) {
            buffer = (getStream() != null ) ? 
                getStream().createFileBuffer(createFileChannel(), 512 * 1024) :
                new FileBuffer(createFileChannel(), ByteBuffer.allocate(512 * 1024));
        }

        if ( existingFile ) {
            try {
                buffer.partition(FILE_HEADER_SIZE);
                buffer.read(1);
                readFileHeader(buffer);
            } catch ( HeaderException header ) {
                throw new IOException(header);
            } catch ( EOFException eof ) {
                throw new HeaderException("truncated header",this);
            }
        } else {
            this.writeJumpList = new WritingSegmentJumpList();
        }
        
        return this;
    }
    
    private FileChannel createFileChannel() throws IOException {
        if ( existingFile ) {
            return new RandomAccessFile(getFile(), "rw").getChannel();
        } else {
            return new FileOutputStream(getFile()).getChannel();
        }
    }
    
    void setJumpList(WritingSegmentJumpList jumps) {
        this.writeJumpList = jumps;
    }

    private long piggybackBufferOptimization(ByteBuffer used) throws IOException {
        long amt = used.remaining();
        int estart = used.limit();
        int position = used.position() - (ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
//  expand buffer
        used.position(position);
        used.limit(estart + (2 * ByteBufferUtils.LONG_SIZE) + ByteBufferUtils.INT_SIZE);
//  place header            
        used.putInt(position, SegmentHeaders.CHUNK_START.getIntValue());
        used.putLong(position + ByteBufferUtils.INT_SIZE, amt);
//  place footer          
        used.putLong(estart, amt);
        used.putLong(estart + ByteBufferUtils.LONG_SIZE, this.getMaximumMarker());
        used.putInt(estart + (2 * ByteBufferUtils.LONG_SIZE), SegmentHeaders.FILE_CHUNK.getIntValue());
//   write it all out
        amt = buffer.writeFully(used);

        writeJumpList.add(buffer.offset());
        return amt;
    }

    public long append(Chunk c, long maxMarker) throws IOException {
        int writeCount = 0;
        buffer.clear();
        if ( this.maxMarker == maxMarker ) {
            throw new IllegalArgumentException("writing the same marker to the log");
        }
        this.maxMarker = maxMarker;
        ByteBuffer[] raw = c.getBuffers();
        if ( //  very specfic optimization to write out buffers as quickly as possible by using extra space in 
                //    passed in buffer creating one large write rather than small header writes
                raw.length == 1 && !raw[0].isReadOnly() && raw[0].isDirect()
                && raw[0].position() > ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE && //  header is a long size and an int chunk marker
                raw[0].capacity() - raw[0].limit() > (2 * ByteBufferUtils.LONG_SIZE) + ByteBufferUtils.INT_SIZE //  footer is a long size for marker long for size and an int chunk marker
                ) {
            return piggybackBufferOptimization(raw[0]);
        } else {
            buffer.clear();
            buffer.partition(ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE);
            long amt = c.remaining();
            buffer.put(SegmentHeaders.CHUNK_START.getBytes());
            buffer.putLong(amt);
            buffer.insert(raw, 1, false);
            buffer.putLong(amt);
            buffer.putLong(maxMarker);
            buffer.put(SegmentHeaders.FILE_CHUNK.getBytes());
            writeCount = raw.length + 2;
            try {
                return buffer.write(writeCount);
            } finally {
                writeJumpList.add(buffer.offset());
            }
        }
    }

    synchronized void prepareForClose() throws IOException {
        if (buffer != null && buffer.isOpen()) {
            buffer.clear();
            buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
            writeJumpList(buffer);
            buffer.write(1);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        totalWrite = 0;
        //  don't need any memory buffers anymore       
        if ( buffer != null && buffer.isOpen() ) {
            totalWrite = buffer.getTotal();
            long delta = System.nanoTime();
            buffer.sync(true);
            delta = System.nanoTime() - delta;
            getStream().recordFsyncLatency(delta);
            buffer.close();
        }

        buffer = null;
    }

    public boolean isClosed() {
        return (buffer == null);
    }

    @Override
    public long size() {
        try {
            return buffer.size();
        } catch (IOException ioe) {
            return -1;
        }
    }

    @Override
    public Iterator<Chunk> iterator() {
        final IntegrityReadbackStrategy reader = new IntegrityReadbackStrategy(buffer);
        try {
            buffer.clear();
            buffer.position(FILE_HEADER_SIZE);
        } catch ( IOException ioe ) {
            return null;
        }
        return new Iterator<Chunk>() {

            @Override
            public boolean hasNext() {
                try {
                    return reader.hasMore(Direction.FORWARD);
                } catch ( IOException ioe ) {
                    return false;
                }
            }

            @Override
            public Chunk next() {
                try {
                    return reader.iterate(Direction.FORWARD);
                } catch ( IOException ioe ) {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        };
    }

    private void writeJumpList(FileBuffer target) throws IOException {
        target.clear();
        target.put(SegmentHeaders.CLOSE_FILE.getBytes());
        long offset = 0;
        for (long jump : writeJumpList) {
            if (target.remaining() < ByteBufferUtils.INT_SIZE
                    + ByteBufferUtils.SHORT_SIZE
                    + ByteBufferUtils.INT_SIZE) {
                target.write(1);
                target.clear();
            }
            target.putInt((int)(jump - offset));
            offset = jump;
        }
        if (writeJumpList.size() < Integer.MAX_VALUE) {
            target.putInt(writeJumpList.size());
        } else {
            target.putInt(-1);
        }
        target.put(SegmentHeaders.JUMP_LIST.getBytes());
    }

    public long position() throws IOException {
        return (buffer == null) ? 0 : buffer.position();
    }

//  assume single threaded
    public long fsync(boolean meta) throws IOException {
        if ( buffer == null ) {
            throw new IOException("segment is closed");
        }
        long pos = buffer.offset();
        long delta = System.nanoTime();
        buffer.sync(meta);
        delta = System.nanoTime() - delta;
        getStream().recordFsyncLatency(delta);
        return pos;
    }
    
    protected void limit(long pos) throws IOException {
        if ( buffer == null ) {
            throw new IOException("segment is closed");
        }
        buffer.position(pos);
        
        buffer.put(SegmentHeaders.CLOSE_FILE.getBytes());
        writeJumpList(buffer);
        buffer.write(1);
        long delta = System.nanoTime();
        buffer.sync(true);
        delta = System.nanoTime() - delta;
        getStream().recordFsyncLatency(delta);
    }
        
    boolean last() throws IOException {
        if ( buffer == null ) {
            throw new IOException("segment is closed");
        }
        buffer.clear();
        buffer.position(FILE_HEADER_SIZE);
        IntegrityReadbackStrategy find = new IntegrityReadbackStrategy(buffer);
        int count = 0;
        try {
            while (find.hasMore(Direction.FORWARD)) {
                try {
                    find.iterate(Direction.FORWARD);
                    count += 1;
                } catch (IOException ioe) {
                    break;
                }
            }
        } finally {
            buffer.clear();
            maxMarker = find.getMaximumMarker();           
            buffer.position(find.getLastValidPosition());
            setJumpList(find.getJumpList());
        }
        
        if ( count == 0 ) {
            return false;
        }
        
        if ( !find.wasClosed() && verifyChunkMark(this.position()) ) {
            this.limit(this.position());
        }
        
        return true;
    }
    
    private boolean verifyChunkMark(long pos) throws IOException {
        buffer.clear();
        buffer.position(pos - ByteBufferUtils.INT_SIZE);
        buffer.partition(4);
        buffer.read(1);
        byte[] code = new byte[4];
        buffer.get(code);
        return SegmentHeaders.FILE_CHUNK.validate(code);
    }    
}
