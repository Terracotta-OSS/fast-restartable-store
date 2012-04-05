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
    Segment currentSegment;
    
    public NIOManager(String home, long segmentSize) throws IOException {
        backend = new NIOStreamImpl(home, segmentSize);
        currentSegment = backend.append();
    }
    
    public void dispose() throws IOException {
        if ( currentSegment != null ) currentSegment.close();
        if ( backend != null ) backend.close();
        currentSegment = null;
        backend = null;
    }
    
    @Override
    public long write(Chunk region) throws IOException {
        if ( currentSegment == null ) throw new IOException("stream is closed");
        long bw = currentSegment.append(region);
        if ( currentSegment.isClosed() ) currentSegment = backend.append();
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
