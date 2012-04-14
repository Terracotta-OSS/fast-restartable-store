/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;

/**
 * A Segment is a logical stride of the Log Stream.  
 * 
 * The actual length and physical
 * storage is implementation specific.
 * 
 * @author mscott
 */
public interface Segment extends Iterable<Chunk> {
    // the length of this segment.  
    
    long length() throws IOException;
    // append a Chunk of data to this segment.  If the chunk appended 
    // is longer than the end of the defined length of this segment, the 
    // behavior is implementation specific
    long append(Chunk c) throws IOException;
    // close this segment.  Nothing can happen to this segment once closed.
    void close() throws IOException;
    // self defined
    boolean isClosed();
    
    int  getSegmentId();
    //  the amount of remaining space in the log segment, either reading or 
    //  writing.  In the write direction, it is possible that this is simply 
    //  a recommendation.
    long remains() throws IOException;
}
