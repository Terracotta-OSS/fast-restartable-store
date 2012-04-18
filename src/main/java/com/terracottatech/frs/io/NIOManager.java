/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;

/**
 * Top level IO Manager using NIO.
 *
 * Assume single threaded operation into NIOManager from a thread created in
 * LogManager
 *
 * @author mscott
 */
public class NIOManager implements IOManager {
    private final File          directory;
    private File          lockFile;
    private FileLock      lock;
    private FileChannel   lastSync;
    private boolean       crashed;
    private final long          segmentSize;
    private UUID                streamid;
    private int                 lastGoodSegment;
    private long                lastGoodPosition;
        
    private static final String BAD_HOME_DIRECTORY = "no home";
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    
    private Stream backend;
    private Segment currentSegment;

    public NIOManager(String home, long segmentSize) throws IOException {
        directory = new File(home);
                
        this.segmentSize = segmentSize;
        checkForCrash();
    }
    
    
    
    private void checkForCrash() throws IOException {
        ByteBuffer check = ByteBuffer.allocate(28);
        if ( crashed ) {
            FileChannel lckChk = new FileInputStream(lockFile).getChannel();
            while ( check.hasRemaining() ) {
                if ( 0 > lckChk.read(check) ) {
                    throw new IOException("bad log marker");
                }
            }
            check.flip();
            streamid = new UUID(check.getLong(),check.getLong());
            lastGoodSegment = check.getInt();
            lastGoodPosition = check.getLong(); 
        } 
    }

    @Override
    public long write(Chunk region) throws IOException {
        if (backend == null) {
            throw new IOException("stream closed");
        }
        if ( currentSegment == null || currentSegment.isClosed() ) currentSegment = backend.append();
        
        return currentSegment.append(region);
    }

    @Override
    public void setLowestLsn(long lsn) throws IOException {
        //  TODO:  Implement compaction
    }

    public void sync() throws IOException {
        if (backend == null) {
            throw new IOException("stream closed");
        }
        long pos = backend.sync();
        ByteBuffer last = ByteBuffer.allocate(28);
        last.putLong(backend.getStreamId().getMostSignificantBits());
        last.putLong(backend.getStreamId().getLeastSignificantBits());
        last.putInt(currentSegment.getSegmentId());
        last.putLong(pos);
        last.flip();
        lastSync.position(0);
        while ( last.hasRemaining() ) lastSync.write(last);
        lastSync.force(false);
            
    }
    
    @Override
    public long seek(long lsn) throws IOException {
        if (backend == null) {
            throw new IOException("stream closed");
        }        backend.seek(lsn);
        currentSegment = null;
        return lsn;
    }
    
    public Chunk read(Direction dir) throws IOException {        
        currentSegment = backend.read(dir);
        
        throw new UnsupportedOperationException("to be implemented");
    }

    @Override
    public void close() throws IOException {
        if ( backend != null ) {
            backend.close();
        }
        if (lock != null) {
            lock.release();
        }
        if (lockFile != null) {
            lockFile.delete();
        }
        currentSegment = null;
        backend = null;
    }
    
    public final void open() throws IOException {
        backend = new NIOStreamImpl(directory, segmentSize);
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(BAD_HOME_DIRECTORY);
        }
        lockFile = new File(directory, "FRS.lck");
        crashed = !lockFile.createNewFile();
        
        FileOutputStream w = new FileOutputStream(lockFile);
        lastSync = w.getChannel();
        lock = lastSync.tryLock();
        if (lock == null) {
            throw new IOException(LOCKFILE_ACTIVE);
        } else {

        }
    }
    

    public boolean isClosed() {
        return (lock != null && lock.isValid());
    }    
}
