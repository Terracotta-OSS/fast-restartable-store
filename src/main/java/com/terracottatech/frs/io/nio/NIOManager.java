/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.UUID;
import static com.terracottatech.frs.util.ByteBufferUtils.LONG_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


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
    private File                lockFile;
    private FileLock            lock;
    private FileChannel         lastSync;
    private final long          segmentSize;
    private UUID                streamid;
    private int                 lastGoodSegment;
    private long                lastGoodPosition;
    private final boolean       writeToLockFile = false;
    private static int          LOCKFILE_INFO_SIZE = LONG_SIZE + LONG_SIZE + INT_SIZE + LONG_SIZE;
        
    private static final String BAD_HOME_DIRECTORY = "no home";
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    
    private NIOStreamImpl backend;

    public NIOManager(String home, long segmentSize) throws IOException {
        directory = new File(home);
                
        this.segmentSize = segmentSize;
        
        open();
    }

    @Override
    public long write(Chunk region) throws IOException {
        if (backend == null) {
            open();
        }    
        
        return backend.append(region);
    }

    @Override
    public void setCurrentMarker(long marker) throws IOException {
        backend.setMarker(marker);
    }

    @Override
    public void setMaximumMarker(long marker) throws IOException {
        backend.setMaximumMarker(marker);
    }

    @Override
    public void setMinimumMarker(long marker) throws IOException {
        backend.setMinimumMarker(marker);
    }

    @Override
    public long getCurrentMarker() throws IOException {
        return backend.getMarker();
    }

    @Override
    public long getMaximumMarker() throws IOException {
        return backend.getMaximumMarker();
    }

    @Override
    public long getMinimumMarker() throws IOException {
        return backend.getMinimumMarker();
    }
    
    

    public void sync() throws IOException {
        if (backend == null) {
            open();
        }
        long pos = backend.sync();

        saveForCrash(pos);    
    }
    
    @Override
    public long seek(long marker) throws IOException {
        if (backend == null) {
            open();
        }        
        backend.seek(marker);
        return marker;
    }
    
    public Chunk read(Direction dir) throws IOException { 
        if (backend == null) {
            open();
        }
                
        return backend.read(dir);
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
        backend = null;
    }
    
    private final void open() throws IOException {        
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(BAD_HOME_DIRECTORY);
        }
        
        backend = new NIOStreamImpl(directory, segmentSize);

        lockFile = new File(directory, "FRS.lck");
        boolean crashed = !lockFile.createNewFile();
        
        FileOutputStream w = new FileOutputStream(lockFile);
        lastSync = w.getChannel();
        lock = lastSync.tryLock();
        if (lock == null) {
            throw new IOException(LOCKFILE_ACTIVE);
        } else {

        }
        if ( crashed ) {
            checkForCrash();
        }
        
        backend.open();
    }
    
    private void saveForCrash(long position) throws IOException {
        if ( writeToLockFile ) {
            ByteBuffer last = ByteBuffer.allocate(LOCKFILE_INFO_SIZE);
            last.putLong(backend.getStreamId().getMostSignificantBits());
            last.putLong(backend.getStreamId().getLeastSignificantBits());
            last.putInt(backend.getSegmentId());
            last.putLong(position);
            last.flip();
            lastSync.position(0);
            while ( last.hasRemaining() ) lastSync.write(last);
            lastSync.force(false);
        }
    }
        
    private void checkForCrash() throws IOException {
        ByteBuffer check = ByteBuffer.allocate(LOCKFILE_INFO_SIZE);
        FileChannel lckChk = new FileInputStream(lockFile).getChannel();
        if ( lckChk.size() != LOCKFILE_INFO_SIZE ) {
            // no good move on
            
            return;
        }
        while ( check.hasRemaining() ) {
            if ( 0 > lckChk.read(check) ) {
                throw new IOException("bad log marker");
            }
        }
        check.flip();
        streamid = new UUID(check.getLong(),check.getLong());
        lastGoodSegment = check.getInt();
        lastGoodPosition = check.getLong(); 
        this.backend.limit(streamid,lastGoodSegment,lastGoodPosition);
    }
    
    public boolean isClosed() {
        return (lock != null && lock.isValid());
    }    
    
    public Future<Void> clean(long timeout) throws IOException {
        backend.seek(0);
        backend.trimLogHead(timeout);
        return new Future<Void>() {

            @Override
            public boolean cancel(boolean bln) {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Void get() throws InterruptedException, ExecutionException {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public boolean isCancelled() {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            @Override
            public boolean isDone() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
            
        };
                
    }
}
