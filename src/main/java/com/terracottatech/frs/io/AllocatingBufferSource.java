/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class AllocatingBufferSource implements BufferSource {
    
    private ByteBuffer soleSource;

    @Override
    public ByteBuffer getBuffer(int size) {
        if ( soleSource == null) {
            soleSource = ByteBuffer.allocateDirect(size);
            return soleSource;
        }
        if ( soleSource.limit() != 0 ) throw new AssertionError("source in use");
        if ( soleSource.capacity() < size ) soleSource = ByteBuffer.allocateDirect(size);
        return (ByteBuffer)soleSource.clear().limit(size);
    }

    @Override
    public void returnBuffer(ByteBuffer buffer) {
   //  probably does nothing
        assert(buffer == soleSource);
        buffer.limit(0);
    }

    @Override
    public void reclaim() {
        if ( soleSource != null ) soleSource.limit(0);
    }
    
    
    
}
