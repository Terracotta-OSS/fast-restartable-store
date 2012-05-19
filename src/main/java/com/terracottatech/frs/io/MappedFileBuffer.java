/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 *
 * @author mscott
 */
public class MappedFileBuffer extends FileBuffer {
    
    public MappedFileBuffer(FileChannel src,FileChannel.MapMode mode, int capacity) throws IOException {
        super(src, src.map(mode, src.position(), src.position() + capacity));
    }

    @Override
    public long getTotal() {
        return super.getTotal();
    }

    @Override
    public void insert(ByteBuffer[] bufs, int loc, boolean writable) throws IOException {
//  yikes, this should not happen, but let's try anyways
        ByteBuffer[] mapped = this.getBuffers();
        if ( mapped.length < loc ) throw new IndexOutOfBoundsException();
        
        Chunk ic = new WrappingChunk(bufs);
        ArrayList<ByteBuffer> front = new ArrayList<ByteBuffer>();
        for (int x=0;x<loc;x++) {
            front.add(mapped[x]);
        }
        Chunk fc = new WrappingChunk(front.toArray(new ByteBuffer[front.size()]));
        ArrayList<ByteBuffer> back = new ArrayList<ByteBuffer>();
        for (int x=loc;x<mapped.length;x++) {
            back.add(mapped[x]);
        }
        Chunk bc = new WrappingChunk(back.toArray(new ByteBuffer[back.size()]));
        this.channel.position(fc.length() + ic.length()).write(bc.getBuffers());
        bc.flip();
        this.channel.position(fc.length()).write(ic.getBuffers());
        ic.flip();
        super.insert(bufs, loc, writable);
    }

    @Override
    public FileBuffer position(long pos) throws IOException {
 //  no context for this in a mapped byte buffer
        this.base = this.channel.map(FileChannel.MapMode.READ_ONLY, pos, pos + this.base.capacity());
        super.position(pos);
        int[] parts = new int[this.ref.length - 1];
        int x = 0;
        for (ByteBuffer b : this.ref ) {
            parts[x++] = b.capacity();
        }
 //  repartition
        this.partition(parts);
        return this;
    }

    @Override
    public long read(int count) throws IOException {
 //  no context for this in a mapped byte buffer
        return 0;
    }

    @Override
    public long write(int count) throws IOException {
 //  no context for this in a mapped byte buffer
        return 0;
    }
    
    
}
