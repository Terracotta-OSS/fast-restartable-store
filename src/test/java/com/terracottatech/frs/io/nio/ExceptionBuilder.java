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
public class ExceptionBuilder implements BufferBuilder {
    
    private final long fullPosition;

    public ExceptionBuilder(long pos) {
        fullPosition = pos;
    }

    @Override
    public FileBuffer createBuffer(FileChannel channel, BufferSource buffer, int size) throws IOException {
        return new FileBuffer(channel, buffer.getBuffer(size)) {

            @Override
            public long writeFully(ByteBuffer buffer) throws IOException {
                if ( channel.position() > fullPosition ) {
                    throw new IOException("crash");
                }
                if ( channel.position() + buffer.remaining() > fullPosition ) {
                    int limit = buffer.limit();
                    buffer.limit(buffer.position() + (int)(fullPosition-channel.position()));
                    ByteBuffer slice = buffer.slice();
                    buffer.position(buffer.limit()).limit(limit);
                    super.writeFully(slice);
                    throw new IOException("crash");
                } else {
                    return super.writeFully(buffer);
                }
            }
            
        };
    }
    
}
