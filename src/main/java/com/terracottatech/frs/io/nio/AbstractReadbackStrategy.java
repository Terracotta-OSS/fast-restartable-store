/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
    private boolean                 consistent = false;


    public AbstractReadbackStrategy() {

    }
    
    
    @Override
    public boolean isConsistent() {
        return consistent;
    }
       
    protected ByteBuffer[] readChunk(Chunk buffer) throws IOException {
        if ( !buffer.hasRemaining() ) {
            return null;
        }
        if ( buffer.remaining() < ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE ) {
            return null;
        }

        int start = buffer.getInt();
// do we see chunk start?  
        if ( !SegmentHeaders.CHUNK_START.validate(start) ) {
            if ( SegmentHeaders.CLOSE_FILE.validate(start) ) {
//  close file magic is in a reasonable place, call this segment consistent
                consistent = true;
            }
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
    
        
    protected ArrayList<Long> readJumpList(Chunk buffer) throws IOException {
        final long LAST_INT_WORD_IN_CHUNK = buffer.remaining()-ByteBufferUtils.INT_SIZE;
        final long LAST_SHORT_WORD_BEFORE_JUMP_MARK = LAST_INT_WORD_IN_CHUNK - ByteBufferUtils.SHORT_SIZE;
        
        int jump = buffer.getInt(LAST_INT_WORD_IN_CHUNK);
        if ( SegmentHeaders.JUMP_LIST.validate(jump) ) {
            int numberOfChunks = buffer.getShort(LAST_SHORT_WORD_BEFORE_JUMP_MARK);
            if ( numberOfChunks < 0 ) {
                return null;
            }
            
            int reach = numberOfChunks * ByteBufferUtils.LONG_SIZE;
            final long EXPECTED_CLOSE_POSITION = LAST_SHORT_WORD_BEFORE_JUMP_MARK - reach - ByteBufferUtils.INT_SIZE;
            if ( EXPECTED_CLOSE_POSITION < 0 ) {
                return null;
            }
            int cfm = buffer.getInt(EXPECTED_CLOSE_POSITION);
            if ( SegmentHeaders.CLOSE_FILE.validate(cfm) ) {
                ArrayList<Long> jumps = new ArrayList<Long>(numberOfChunks);
                for (int x=0;x<numberOfChunks;x++) {
                    jumps.add(buffer.getLong(EXPECTED_CLOSE_POSITION + ByteBufferUtils.INT_SIZE + (x*ByteBufferUtils.LONG_SIZE)));
                }
                return jumps;
            }
        }
        return null;
    }

}
