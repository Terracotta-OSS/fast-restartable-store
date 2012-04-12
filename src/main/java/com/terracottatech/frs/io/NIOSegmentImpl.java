/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author mscott
 */
class NIOSegmentImpl implements Segment {

    private final NIOStreamImpl parent;
    private final File          src;
    private final int           segNum;
    private final Direction     direction;

    private FileChannel         segment;
    private Chunk               readBuffer;
    private long                limit = 10 * 1024 * 1024;
    private static final byte[] LF_MAGIC = "%flf".getBytes();
    private static final byte[] CF_MAGIC = "!ctl".getBytes();
    private static final byte[] FILE_CHUNK_MAGIC = "~fc~".getBytes();
    private static final short  IMPL_NUMBER = 02;
    
    private List<Chunk> chunks;
    private final ByteBuffer header = ByteBuffer.allocate(12);
    private UUID streamid;
    
    public NIOSegmentImpl(NIOStreamImpl p, Direction dir, File file, long segSize) {
        this.parent = p;
        this.src = file;
        this.limit = segSize;
        this.direction = dir;
        this.segNum = p.convertSegmentNumber(file);
    }

    NIOSegmentImpl openForReading(ChunkSource reader) throws IOException {
        readBuffer = reader.wrapFile(src);
        byte[] lfm = new byte[4];
        readBuffer.get(lfm);
        assert(Arrays.equals(lfm,LF_MAGIC));
        short impl = readBuffer.getShort();
                
        int checkSeg = readBuffer.getInt();
        assert(segNum == checkSeg);

        assert(impl == IMPL_NUMBER);
        streamid = new UUID(readBuffer.getLong(), readBuffer.getLong());   
                
        if ( this.limit < src.length() ) {
            ((FileChunk)readBuffer).setLimit(this.limit);
        }
        
        chunks = queueChunks(readBuffer);
         if ( direction == Direction.REVERSE ) Collections.reverse(chunks);
         
         return this;
   }

    //  open and write the header.
    NIOSegmentImpl openForWriting() throws IOException {
        segment = new FileOutputStream(src).getChannel();
        ByteBuffer allocate = ByteBuffer.allocate(26);
        this.streamid = parent.getStreamId();

        allocate.put(LF_MAGIC);
        allocate.putShort(IMPL_NUMBER);
        allocate.putInt(segNum);
        allocate.putLong(streamid.getMostSignificantBits());
        allocate.putLong(streamid.getLeastSignificantBits());
        allocate.flip();
        segment.write(allocate);
        return this;
    }

    // getBuffers does the bulk of the work defining header for LogRecord
    @Override
    public long append(Chunk c) throws IOException {
        if ( segment == null ) throw new IOException("segment not open for writing");
        header.clear();
        long amt = c.remaining();
        assert(amt == c.length());
        header.putLong(amt);
        header.flip();
        long wl = segment.write(header);
        while ( wl < amt + 8) {
            wl += segment.write(c.getBuffers());
        }
        header.clear();
        assert(wl == amt + 8);
        header.putLong(wl-8);
        header.put(FILE_CHUNK_MAGIC);
        header.flip();
        wl += segment.write(header);
        if (segment.position() >= limit) {
            close();
        }
        return wl;
    }

    @Override
    public void close() throws IOException {
        readBuffer = null;
        if (segment == null || !segment.isOpen()) {
            return;
        }
        ByteBuffer close = ByteBuffer.allocate(4);
        close.put(CF_MAGIC);
        close.flip();
        segment.write(close);
        //  TODO: is this force neccessary?  not sure, research
        segment.force(false);
        segment.close();
        segment = null;
    }
//  assume single threaded
    public long fsync() throws IOException {
        long pos = segment.position();
        segment.force(false);
        return pos;
    }
    
    int getSegmentNumber() {
        return segNum;
    }
    
    UUID getStreamId() {
        return streamid;
    }

    @Override
    public boolean isClosed() {
        
        return (readBuffer == null && segment == null);
    }

    @Override
    public Iterator<Chunk> iterator() {
        return chunks.iterator();
    }
    
    private List<Chunk> queueChunks(Chunk bb) {
        List<Chunk> list = new ArrayList<Chunk>();
        while (bb.hasRemaining() && bb.remaining() >= 22) {
            long length = bb.getLong();
            for ( ByteBuffer b : bb.getBuffers(length) ) list.add(new WrappingChunk(b));
            long confirm = bb.getLong();
            byte[] fcm = new byte[4];
            bb.get(fcm);
            assert(length == confirm);
            assert(Arrays.equals(fcm,FILE_CHUNK_MAGIC));
        }
        return list;
    }

    @Override
    public long length() throws IOException {
        return segment.size();
    }

    @Override
    public long remains() throws IOException {
        return limit - segment.position();
    }
}
