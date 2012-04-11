/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.IOException;

/**
 * Top level IO Manager using NIO.
 *
 * Assume single threaded operation into NIOManager from a thread created in
 * LogManager
 *
 * @author mscott
 */
public class NIOManager implements IOManager {

    private final Stream backend;
    private Segment currentSegment;

    public NIOManager(String home, long segmentSize) throws IOException {
        backend = new NIOStreamImpl(home, segmentSize);

    }

    public void dispose() throws IOException {
        if (currentSegment != null) {
            currentSegment.close();
        }
        if (backend != null) {
            backend.close();
        }
        currentSegment = null;
    }

    @Override
    public long write(Chunk region) throws IOException {
        if (backend == null) {
            throw new IOException("stream closed");
        }
        if (currentSegment == null) {
            currentSegment = backend.append();
        }
        long bw = currentSegment.append(region);
        if (currentSegment.isClosed()) {
            currentSegment = backend.append();
        }
        return bw;
    }

    @Override
    public void setLowestLsn(long lsn) throws IOException {
        //  TODO:  Implement compaction
    }

    public void sync() throws IOException {
        if (backend == null) {
            throw new IOException("stream closed");
        }
        backend.sync();
    }
    
    @Override
    public long seek(long lsn) throws IOException {
        backend.seek(lsn);
        return lsn;
    }
    
    public Iterable<Chunk> read(Direction dir) throws IOException {
        Segment seg = backend.read(dir);
        
        return seg;
    }

    @Override
    public void close() throws IOException {
        this.dispose();
    }
    
    
    
}
