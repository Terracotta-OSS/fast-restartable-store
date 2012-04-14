/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.util.UUID;

/**
 * Logical continuous Log Stream.
 * @author mscott
 */
public interface Stream {
    
    /* stream back segments in forward or reverse direction  */
    void seek(long loc) throws IOException;
    
    Segment read(Direction dir) throws IOException;
    
//    long write(Chunk c) throws IOException;
    
    /* close previous segment if any, provide a new segment for appending  */
    
    Segment append() throws IOException;
    
    UUID getStreamId();
    
    long sync() throws IOException;
    
    void close() throws IOException;
}
