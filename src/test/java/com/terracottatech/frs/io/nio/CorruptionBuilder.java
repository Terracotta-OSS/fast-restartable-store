/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.BufferBuilder;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.FileBuffer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class CorruptionBuilder implements BufferBuilder {
    
    private byte[] corrupt;
    private long location;

    public CorruptionBuilder(byte[] pattern, long location) {
        this.corrupt = pattern;
        this.location = location;
    }

    @Override
    public FileBuffer createBuffer(final FileChannel channel, final BufferSource buffer, int size) throws IOException {
        return new FileBuffer(channel, buffer.getBuffer(size)) {

            @Override
            public long writeFully(ByteBuffer buffer) throws IOException {
                if ( buffer.remaining() + channel.position() > location ) {
                    int loc = (int)(location - channel.position()) + buffer.position();
                    for(int x=0;x<corrupt.length;x++) {
                        if ( loc + x < buffer.capacity() ) {
                            buffer.put(loc + x, corrupt[x]);
                        }
                    }
                }
                return super.writeFully(buffer);
            }
            
        };
    }
    
}
