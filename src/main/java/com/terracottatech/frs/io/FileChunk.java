/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wrap a file in a chunk for easy access.
 * @author mscott
 */
public class FileChunk extends AbstractChunk {
    
    File src;
    ByteBuffer reserve;
    ByteBuffer[] ref;
    
    public FileChunk(File c, ByteBuffer reserve) throws IOException {
        this.src = c;
        this.reserve = reserve;
        init();
    }

    private void init() throws IOException {
        if ( reserve.remaining() >= src.length() ) {
            new FileInputStream(src).getChannel().read(reserve);
            reserve.flip();
            ref = new ByteBuffer[]{reserve};
        } else {
            ByteBuffer end = ByteBuffer.allocate((int)(src.length() - reserve.remaining()));
            ByteBuffer[] grab = new ByteBuffer[] {reserve,end};
            new FileInputStream(src).getChannel().read(grab);
            reserve.flip();
            end.flip();
            ref = new ByteBuffer[]{reserve,end};
        }
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return ref;
    }

    @Override
    public long length() {
       return src.length();
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
    
}
