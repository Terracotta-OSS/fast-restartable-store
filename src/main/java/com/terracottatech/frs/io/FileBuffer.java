/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Wrap a file in a chunk for easy access.
 * @author mscott
 */
public class FileBuffer extends AbstractChunk implements Closeable {
    
    private final FileChannel channel;
    private final ByteBuffer  base;
    private int               mark = 0;
    private long              total = 0;
    private long              offset = 0;
    private ByteBuffer[]      ref;
    
    public FileBuffer(FileChannel channel, ByteBuffer src) throws IOException {
        this.channel = channel;
        this.base = src;
        this.ref = new ByteBuffer[]{base.duplicate()};
        this.offset = 0;
    }

    public FileBuffer(File src) throws IOException {
        this.channel = new FileInputStream(src).getChannel();
        if ( channel.size() > Integer.MAX_VALUE ) throw new RuntimeException("integer overflow error");
        this.base = ByteBuffer.allocate((int)channel.size());
        this.ref = new ByteBuffer[]{base.duplicate()};
    }
    
    public long getTotal() {
        return total;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return ref;
    }

    public long position() {
        return offset + (length()-remaining());
    }
    
    public long offset() {
        return offset;
    }
    
    public int capacity() {
        return base.capacity();
    }
    
    public void bufferMove(int src,int dest,int length) {
        ByteBuffer from = (ByteBuffer)base.duplicate().position(src).limit(src+length);
        ByteBuffer to = (ByteBuffer)base.duplicate().position(dest).limit(dest+length);
        to.put(from);
    }
    
    public FileBuffer position(long pos) throws IOException {
        if ( pos < 0 ) channel.position(channel.size() + pos);
        else channel.position(pos);
        return this;
        
    }
    
    public FileBuffer partition(int...pos) {
        ByteBuffer target = base.duplicate();
        
        ArrayList<ByteBuffer> sections = new ArrayList<ByteBuffer>();
        for (int p : pos) {
            if ( p > target.limit() ) {
                throw new BufferUnderflowException();
            } else {
                sections.add((ByteBuffer)target.slice().limit(p-target.position()));
                target.position(target.position() + p);
            }
        }
        sections.add((ByteBuffer)target.slice());
        
        ref = sections.toArray(new ByteBuffer[sections.size()]);
        mark = 0; //  reset the mark count so we start reading from the start again
        return this;
    }
    
    @Override
    public void clear() {
        ref = new ByteBuffer[]{base.duplicate()};
        mark = 0;
    }
    
    public long read(int count) throws IOException {
        long lt = 0;
        offset = channel.position();
        for (int x=mark;x<mark + count;x++) {
            if ( ref[x].isReadOnly() ) {
                ref[x] = ref[x].duplicate();
                ref[x].position(ref[x].limit());
            } else {
                assert(ref[x].position() == 0);
            }
        }
        while (ref[mark+count-1].hasRemaining()) {
            lt += channel.read(ref,mark,count);
        }
        for (int x=mark;x<mark + count;x++) {
            ref[x].flip();
        }
        mark += count;
        total += lt;
        return lt;
    }
    
    public long write(int count) throws IOException {
        long lt = 0;
        for (int x=mark;x<mark + count;x++) {
            if ( !ref[x].isReadOnly() ) ref[x].flip();
            assert(ref[x].position() == 0);
        }
        while (ref[mark+count-1].hasRemaining()) {
            lt += channel.write(ref,mark,count);
        }
        offset = channel.position();
        mark += count;
        total += lt;
        return lt;
    }
    
    public void append(ByteBuffer[] bufs) {
        int pos = ref.length;
        ref = Arrays.copyOf(ref,ref.length + bufs.length);
        for (int x=pos;x<bufs.length+pos;x++) {
            ref[pos+x] = bufs[x].asReadOnlyBuffer();
        }
    }
    
    public void insert(ByteBuffer[] bufs,int loc) {
        int len = ref.length;
        ref = Arrays.copyOf(ref,ref.length + bufs.length);
        System.arraycopy(ref, loc, ref, loc + bufs.length, len - loc);
        for (int x=0;x<bufs.length;x++) {
            ref[loc+x] = bufs[x].asReadOnlyBuffer();
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
        ref = null;
    }
    
    

}
