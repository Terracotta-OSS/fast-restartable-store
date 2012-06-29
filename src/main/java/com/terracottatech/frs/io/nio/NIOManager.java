/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.*;
import com.terracottatech.frs.util.NullFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Formatter;
import java.util.UUID;
import java.util.concurrent.Future;

import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;
import static com.terracottatech.frs.util.ByteBufferUtils.LONG_SIZE;


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
    private File                backupLockFile;
    private FileLock            lock;
    private FileChannel         lastSync;
    private final long          segmentSize;
    private long                memorySize;
    private UUID                streamid;
    private int                 lastGoodSegment;
    private long                lastGoodPosition;
    private final boolean       writeToLockFile = false;
    private static int          LOCKFILE_INFO_SIZE = LONG_SIZE + LONG_SIZE + INT_SIZE + LONG_SIZE;
        
    private static final String BAD_HOME_DIRECTORY = "no home";
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    private static final String BACKUP_LOCKFILE = "frs.backup.lck";
    
    private NIOStreamImpl backend;
    private long written = 1;
    private long read = 1;
    private long writeTime = 1;
    private long parts = 1;
    private long requests = 1;
    
    private volatile boolean readOpsAllowed = true;
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);

    public NIOManager(String home, long segmentSize) throws IOException {
        this(home,segmentSize,segmentSize * 4);
    }    
    
    public NIOManager(String home, long segmentSize, long memorySize) throws IOException {
        directory = new File(home);
                
        this.segmentSize = segmentSize;
        
        this.memorySize = memorySize;
        
        open();
    }
    
    public NIOManager(Configuration config) throws IOException {
        this(config.getDBHome().getAbsolutePath(),
            config.getLong(FrsProperty.IO_NIO_SEGMENT_SIZE),
            config.getLong(FrsProperty.IO_NIO_MEMORY_SIZE));

        backend.memorySpinsToFail(config.getInt(FrsProperty.IO_NIO_MEMORY_SPINS));
        backend.memoryTimeToWait(config.getLong(FrsProperty.IO_NIO_MEMORY_TIMEOUT));
        String bufferBuilder = config.getString(FrsProperty.IO_NIO_BUFFER_BUILDER);
        if ( bufferBuilder != null ) {
            try {
                backend.setBufferBuilder((BufferBuilder)Class.forName(bufferBuilder).newInstance());
            } catch ( ClassNotFoundException cnf ) {
                LOGGER.warn("custom builder", cnf);
            } catch ( IllegalAccessException iae ) {
                LOGGER.warn("custom builder", iae);
            } catch ( InstantiationException ie ) {
                LOGGER.warn("custom builder", ie);
            } catch ( ClassCastException cce ) {
                LOGGER.warn("custom builder", cce);
            }
        }
    }

    @Override
    public long write(Chunk region) throws IOException {
        if (backend == null) {
            open();
        }    
        
        long blit = System.nanoTime();
        long w = backend.append(region);
        blit = System.nanoTime() - blit;
        written += w;
        writeTime += blit;
        parts += region.getBuffers().length;
        requests += 1;
        return w;
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
        if ( backend == null ) return 0;
        return backend.getMarker();
    }

    @Override
    public long getMaximumMarker() throws IOException {
        if ( backend == null ) return 0;
        return backend.getMaximumMarker();
    }

    @Override
    public long getMinimumMarker() throws IOException {
        if ( backend == null ) return 0;
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
        assert(readOpsAllowed);

        if (backend == null) {
            open();
        }    
        
        backend.seek(marker);
        return marker;
    }
    
    @Override
    public Chunk read(Direction dir) throws IOException { 
        assert(readOpsAllowed);
        
        if (backend == null) {
            open();
        }
                
        Chunk c = backend.read(dir);
        if ( c!= null ) read += c.remaining();
        
        return c;
    }

    @Override
    public void close() throws IOException {
        if ( backend != null ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug("==PERFORMANCE(iostats)== " + getStatistics());
            }
            backend.close();
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(iowrite)==  written: %.2f MB in %d parts over %d requests.\n==PERFORMANCE(iowrite)==  total time: %.3f msec -- rate: %.3f MB/s - %.4f B/part - %.2f parts/request",
                    written/(1024d*1024d),parts,requests,writeTime*1e-3,(written*1e9)/(writeTime*1024d*1024d),(written*1d)/(parts),(parts*1d)/requests).out().toString());
        }
        if (lock != null) {
            lock.release();
            lastSync.close();
            lock = null;
        }
        if (lockFile != null) {
            if ( !lockFile.delete() ) {
                throw new IOException("lock file cannot be deleted");
            }
            lockFile = null;
        }
        backend = null;
    }
    
    private void open() throws IOException {        
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException(BAD_HOME_DIRECTORY);
        }
        
        backend = new NIOStreamImpl(directory, segmentSize, memorySize);

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

        backupLockFile = new File(directory, BACKUP_LOCKFILE);
        backupLockFile.createNewFile();

        backend.open();
    }

    @Override
    public String toString() {
        return "NIO - " + directory.getAbsolutePath();
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
        return !(lock != null && lock.isValid());
    }
    
    

    @Override
    public synchronized IOStatistics getStatistics() throws IOException {
        readOpsAllowed = false;
        try {
            return new NIOStatistics(directory, backend.getTotalSize(), backend.findLogTail(), written, read);
        } finally {
            readOpsAllowed = true;
        }
    }
    
    @Override
    public synchronized Future<Void> clean(long timeout) throws IOException {        
        readOpsAllowed = false;
        FileOutputStream fos = new FileOutputStream(backupLockFile);
        FileChannel channel = fos.getChannel();
        FileLock backupLock = channel.lock(0, Long.MAX_VALUE, false);
        try {
            if (backupLock == null) {
                LOGGER.info("Unable to lock backup lockfile. Delaying log file cleanup until the backup is complete.");
                return NullFuture.INSTANCE;
            }
            backend.trimLogTail(timeout);
        } finally {
            if (backupLock != null) {
                backupLock.release();
            }
            fos.close();
            readOpsAllowed = true;
        }
        
        return NullFuture.INSTANCE;
    }
}
