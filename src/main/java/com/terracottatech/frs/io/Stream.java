/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;

/**
 * Logical continuous Log Stream.
 * @author mscott
 */
public interface Stream {
    
    /* stream back segments in forward or reverse direction  */
    void seek(long loc) throws IOException;
    
    Segment read(Direction dir) throws IOException;
    
    /* close previous segment if any, provide a new segment for appending  */
    
    Segment append() throws IOException;
    
    void sync() throws IOException;
    
    void close() throws IOException;
}
