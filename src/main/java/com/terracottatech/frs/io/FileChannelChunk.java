/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Wrap a file in a chunk for easy access.
 * @author mscott
 */
public class FileChannelChunk extends AbstractChunk {
    
    private final FileChannel src;
    private final ByteBuffer reserve;
    private final long length;
    private ByteBuffer[] ref;
    
    public FileChannelChunk(FileChannel c, ByteBuffer reserve) throws IOException {
        this.src = c;
        this.length = c.size();
        this.reserve = reserve;
        init();
    }

    private void init() throws IOException {
        long read = 0;
        if ( reserve.remaining() >= src.size() ) {
            ref = new ByteBuffer[]{reserve};
        } else {
            ByteBuffer end = ByteBuffer.allocate((int)(src.size() - reserve.remaining()));
            ref = new ByteBuffer[] {reserve,end};
        }
        while ( src.size() > read ) {
            read += src.read(ref);
        }
        assert(read == src.size());
        for  ( ByteBuffer b : ref ) {
            b.flip();
        }
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return ref;
    }

    @Override
    public long length() {
       return length;
    }
    
    public ByteBuffer getReserve() {
        return reserve;
    }
    
    public void setLimit(long limit) {
        if ( ref.length == 1 || ref[0].capacity() > limit ) {
            ref[0].limit((int)limit);
        } else {
            ref[1].limit((int)limit - ref[0].capacity());
        }
    }
    
    public void partition(long...pos) {
        ArrayList<ByteBuffer> sections = new ArrayList<ByteBuffer>();
        int section = 0;
        ByteBuffer play = (ByteBuffer)ref[section++].duplicate();
        long offset = 0;
        long last = 0;
        for ( long p : pos) {
            play.position((int)(last-offset));
            if ( p > offset + play.capacity() ) {
                offset += play.capacity();
                ByteBuffer refp = null;
                if ( play.hasRemaining() ) {
                    refp = ByteBuffer.allocate((int)(p-last));
                    refp.put(play);
                }
                play = ref[section++].duplicate();                    
                play.limit((int)(p-offset));
                if ( refp == null ) refp = play.slice();
                else refp.put(play);
                sections.add(refp);
            } else {
                play.limit((int)(p-offset));
                sections.add(play.slice());
            }
            last = p;
        }
        play.position((int)(last-offset));
        play.limit(play.capacity());
        sections.add(play.slice());
        
        ref = sections.toArray(new ByteBuffer[sections.size()]);
    }
    
}
