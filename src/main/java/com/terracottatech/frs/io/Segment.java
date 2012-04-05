/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.util.Iterator;

/**
 * A Segment is a logical stride of the Log Stream.  
 * 
 * The actual length and physical
 * storage is implementation specific.
 * 
 * @author mscott
 */
public interface Segment extends Iterable<ChunkIntent> {
    // the length of this segment.  
    
    long length() throws IOException;
    
    // produce an iterator either forward or backward threw the log.
    Iterator<ChunkIntent> iterator(Direction dir);
    // append a Chunk of data to this segment.  If the chunk appended 
    // is longer than the end of the defined length of this segment, the 
    // behavior is implementation specific
    long append(Chunk c) throws IOException;
    // close this segment.  Nothing can happen to this segment once closed.
    void close() throws IOException;
    // self defined
    boolean isClosed();
    //  the amount of remaining space in the log segment, either reading or 
    //  writing.  In the write direction, it is possible that this is simply 
    //  a recommendation.
    long remains() throws IOException;
}
