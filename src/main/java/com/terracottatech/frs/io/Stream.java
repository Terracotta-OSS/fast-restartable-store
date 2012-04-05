/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.util.Iterator;

/**
 * Logical continuous Log Stream.
 * @author mscott
 */
public interface Stream {
    
    /* stream back segments in forward or reverse direction  */
    
    Iterator<Segment> iterator(Direction dir);
    
    /* close previous segment if any, provide a new segment for appending  */
    
    Segment append() throws IOException;
    
    void sync() throws IOException;
    
    void close() throws IOException;
}
