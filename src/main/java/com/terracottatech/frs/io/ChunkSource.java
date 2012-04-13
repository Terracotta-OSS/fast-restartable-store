/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class ChunkSource {
    
    
    
    public Chunk allocateChunk(int size) {
        return new WrappingChunk(ByteBuffer.allocate(size));
    }
    
    public void releaseChunk(Chunk c) {
        if ( c instanceof WrappingChunk ) {

        } else if ( c instanceof FileChunk ) {
            
        }
    }
    
    public Chunk wrapFile(File f) throws IOException {
        return new FileChunk(f,ByteBuffer.allocateDirect((int)f.length()));
    }
    
    public Chunk wrapFileChannel(FileChannel channel) throws IOException {
        return new FileChannelChunk(channel,ByteBuffer.allocateDirect((int)channel.size()));
    }
    
}
