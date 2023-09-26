/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
