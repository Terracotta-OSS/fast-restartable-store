/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.LogRegionFactory;
import java.io.IOException;
import java.util.Iterator;

/**
 * Top level IO Manager using NIO.  
 * 
 * Assume single threaded operation into NIOManager from 
 * a thread created in LogManager
 * 
 * @author mscott
 */
public class NIOManager implements IOManager {
  
    Stream  backend;
    Segment current_segment;
    
    public NIOManager(String home, long segment_size) throws IOException {
        backend = new NIOStreamImpl(home, segment_size);
        current_segment = backend.append();
    }
    
    public void dispose() throws IOException {
        if ( current_segment != null ) current_segment.close();
        if ( backend != null ) backend.close();
        current_segment = null;
        backend = null;
    }
    
    @Override
    public long write(Chunk region) throws IOException {
        if ( current_segment == null ) throw new IOException("stream is closed");
        long bw = current_segment.append(region);
        if ( current_segment.isClosed() ) current_segment = backend.append();
        return bw;
    }

    @Override
    public void setLowestLsn(long lsn) throws IOException {
        //  TODO:  Implement compaction
    }

    public void sync() throws IOException {
        if ( backend == null ) throw new IOException("stream closed");
        backend.sync();
    }

    @Override
    public <T> Iterator<T> reader(LogRegionFactory<T> as) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
