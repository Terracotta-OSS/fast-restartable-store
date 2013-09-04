/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author mscott
 */
public class SimulationFileBuffer extends FileBuffer {
    
    BufferFilter filters;

    public SimulationFileBuffer(FileChannel channel, ByteBuffer src) throws IOException {
        super(channel, src);
        filters = GlobalFilters.getFilters();
    }

    @Override
    public long writeFully(ByteBuffer buffer) throws IOException {
        BufferFilter f = filters;
        while ( f != null ) {
            if ( !f.filter(buffer) ) {
                return 0;
            }
            f = f.next();
        }
        return super.writeFully(buffer);
    }
    
    public void addFilter(BufferFilter filter) {
        if ( filters == null ) filters = filter;
        else filters.add(filter);
    }
    
    public void setFilters(BufferFilter filter) {
        filters = filter;
    }

}
