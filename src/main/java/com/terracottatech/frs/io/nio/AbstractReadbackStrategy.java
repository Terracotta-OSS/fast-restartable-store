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
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author mscott
 */
abstract class AbstractReadbackStrategy implements ReadbackStrategy {
    private volatile boolean                 closedDetected = false;


    public AbstractReadbackStrategy() {

    }
        
    protected boolean isCloseDetected() {
        return closedDetected;
    }
       
    protected ByteBuffer[] readChunk(Chunk buffer) throws IOException {
        if ( !buffer.hasRemaining() ) {
            return null;
        }
        if ( buffer.remaining() < ByteBufferUtils.INT_SIZE ) {
            return null;
        }

        int start = buffer.getInt();
// do we see chunk start?  
        if ( !SegmentHeaders.CHUNK_START.validate(start) ) {
            if ( SegmentHeaders.CLOSE_FILE.validate(start) ) {
//  close file magic is in a reasonable place, call this segment consistent
                closedDetected = true;
            }
            return null;
        }
        
        if ( buffer.remaining() < ByteBufferUtils.LONG_SIZE ) {
            return null;
        }
        
        long length = buffer.getLong();
        if ( buffer.remaining() < length + ByteBufferUtils.LONG_SIZE + ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE ) {
            return null;
        }
        ByteBuffer[] targets = buffer.getBuffers(length);
//  confirm lengths match
        if ( length != buffer.getLong() ) {
            return null;
        }
//  this long is the max marker at the time of chunk write
        buffer.getLong();
// confirm file chunk magic        
        if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt()) ) {
            return null;
        }
        
        return targets;
    }       
        
    protected long[] readJumpList(ByteBuffer buffer) throws IOException {
        final int LAST_INT_WORD_IN_CHUNK = buffer.position()+buffer.remaining()-ByteBufferUtils.INT_SIZE;
        final int LAST_INT_WORD_BEFORE_JUMP_MARK = LAST_INT_WORD_IN_CHUNK - ByteBufferUtils.INT_SIZE;
        
        if ( !buffer.hasRemaining() ) {
            return null;
        }
        
        int jump = buffer.getInt(LAST_INT_WORD_IN_CHUNK);
        if ( SegmentHeaders.JUMP_LIST.validate(jump) ) {
            int numberOfChunks = buffer.getInt(LAST_INT_WORD_BEFORE_JUMP_MARK);
            if ( numberOfChunks < 0 ) {
                return null;
            }
            
            int reach = numberOfChunks * ByteBufferUtils.INT_SIZE;
            final int EXPECTED_CLOSE_POSITION = LAST_INT_WORD_BEFORE_JUMP_MARK - reach - ByteBufferUtils.INT_SIZE;
            if ( EXPECTED_CLOSE_POSITION < 0 ) {
                return null;
            }
            int cfm = buffer.getInt(EXPECTED_CLOSE_POSITION);
            if ( SegmentHeaders.CLOSE_FILE.validate(cfm) ) {
                long[] jumps = new long[numberOfChunks];
                long last = 0;
                for (int x=0;x<numberOfChunks;x++) {
                  int value = buffer.getInt(EXPECTED_CLOSE_POSITION + ByteBufferUtils.INT_SIZE + (x*ByteBufferUtils.INT_SIZE));
                  if ( value < 0 ) {
                    return null;
                  } else {
                    last += value;
                    jumps[x] = last;
                  }
                }
                closedDetected = true;
                return jumps;
            }
        }
        return null;
    }
}
