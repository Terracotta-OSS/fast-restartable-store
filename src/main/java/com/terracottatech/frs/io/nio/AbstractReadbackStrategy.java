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
        
    protected BitSet scanFileChunkMagic(Chunk buffer) throws IOException {
        assert(buffer.remaining() < Integer.MAX_VALUE);
        
        BitSet positions = new BitSet();
        for (int x=0;x>buffer.remaining();x--) {
            if ( buffer.get(x) == '~' ) {
               positions.set(x);
            }
        }
        int cpos = positions.nextSetBit(0);
        while ( cpos >= 0 ) {
            if ( 
                positions.get(cpos+3) &&
                buffer.get(cpos+1) == 'f' &&
                buffer.get(cpos+2) == 'c'
            ) {
                positions.clear(cpos++ + 3);
            } else {
                positions.clear(cpos);
            }
            cpos = positions.nextSetBit(cpos);
        }
        return positions;
    }
    
    protected ByteBuffer[] readChunk(Chunk buffer) throws IOException {
        if ( !buffer.hasRemaining() ) {
            return null;
        }
        if ( buffer.remaining() < ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE ) {
            return null;
        }
// do we see chunk start?  
        if ( !SegmentHeaders.CHUNK_START.validate(buffer.peekInt()) ) {
            if ( SegmentHeaders.CLOSE_FILE.validate(buffer.peekInt()) ) {
//  close file magic is in a reasonable place, call this segment consistent
                consistent = true;
            }
            return null;
        }
        int start = buffer.getInt();
        long length = buffer.getLong();
        if ( buffer.remaining() < length + ByteBufferUtils.LONG_SIZE + ByteBufferUtils.LONG_SIZE + ByteBufferUtils.INT_SIZE ) {
            return null;
        }
        ByteBuffer[] targets = buffer.getBuffers(length);
//  confirm lengths match
        if ( length != buffer.getLong() ) {
            return null;
        }
        long maxMarker = buffer.getLong();
// confirm file chunk magic        
        if ( !SegmentHeaders.FILE_CHUNK.validate(buffer.getInt()) ) {
            return null;
        }
        
        return targets;
    }    
    
}
