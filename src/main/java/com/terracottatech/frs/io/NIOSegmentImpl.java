/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.UUID;

/**
 *
 * @author mscott
 */
public class NIOSegmentImpl implements Segment {
    
    FileChannel  segment;
    long limit = 10 * 1024 * 1024;
    UUID uid = UUID.randomUUID();
    static byte[] LF_MAGIC = "%flf".getBytes();
    static byte[] CF_MAGIC = "!ctl".getBytes();
    static short  IMPL_NUMBER = 02;
    

    public NIOSegmentImpl(File file, long seg_size, boolean reading) throws IOException {
        limit = seg_size;
        if ( reading ) openForReading(file);
        else openForWriting(file);
    }
    
    private void openForReading(File p)  throws IOException {
        segment = new FileInputStream(p).getChannel();
    }
    
    //  open and write the header.
    private void openForWriting(File p)  throws IOException {
        segment = new FileOutputStream(p).getChannel();
        ByteBuffer allocate = ByteBuffer.allocate(22);

        allocate.put(LF_MAGIC);
        allocate.putShort(IMPL_NUMBER);
        allocate.putLong(uid.getLeastSignificantBits());
        allocate.putLong(uid.getMostSignificantBits());
        segment.write(allocate);

    }    
    
    // getBuffers does the bulk of the work defining header for LogRecord
    @Override
    public long append(Chunk c) throws IOException {
        long wl = segment.write(c.getBuffers());
        if ( segment.position() >= limit ) close();
        return wl;
    }

    @Override
    public void close() throws IOException {
        if ( !segment.isOpen() ) return;
        ByteBuffer close = ByteBuffer.allocate(4);
        close.put(CF_MAGIC);
        close.flip();
        segment.write(close);
        //  TODO: is this force neccessary?  not sure, research
        segment.force(false);
        segment.close();
    }
    
    public void fsync() throws IOException {
        segment.force(false);
    }

    @Override
    public boolean isClosed() {
        return !segment.isOpen();
    }

    @Override
    public Iterator<ChunkIntent> iterator(Direction dir) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long length() throws IOException {
        return segment.size();
    }

    @Override
    public long remains() throws IOException {
        return limit - segment.position();
    }

    @Override
    public Iterator<ChunkIntent> iterator() {
        return iterator(Direction.getDefault());
    }
    
}
