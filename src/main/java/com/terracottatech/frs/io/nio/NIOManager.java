/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.BufferBuilder;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.util.NullFuture;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.Future;



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
    private final long          segmentSize;
    private long                memorySize;
        
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    
    private NIOStreamImpl backend;
    private long written = 1;
    private long read = 1;
    private long writeTime = 1;
    private long parts = 1;
    private long requests = 1;

    private int snapshots = 0;
    
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
    
    void setBufferBuilder(BufferBuilder builder) {
        if ( backend != null ) {
            backend.setBufferBuilder(builder);
        }
    }
    
    BufferBuilder getBufferBuilder() {
        if ( backend == null ) return null;
        return backend.getBufferBuilder();
    }    

    @Override
    public long write(Chunk region, long marker) throws IOException {
        if (backend == null) {
            throw new IOException("stream is closed");
        }    
        
        long blit = System.nanoTime();
        long w = backend.append(region, marker);
        blit = System.nanoTime() - blit;
        written += w;
        writeTime += blit;
        parts += region.getBuffers().length;
        requests += 1;
        return w;
    }

    @Override
    public void setMinimumMarker(long marker) throws IOException {
        if ( backend == null ) {
            throw new IOException("stream is closed");
        }
        backend.setMinimumMarker(marker);
    }

    @Override
    public long getCurrentMarker() throws IOException {
        if ( backend == null ) {
            throw new IOException("stream is closed");
        }
        return backend.getMarker();
    }

    @Override
    public long getMinimumMarker() throws IOException {
        if ( backend == null ) {
            throw new IOException("stream is closed");
        }
        return backend.getMinimumMarker();
    }

    

    @Override
    public void sync() throws IOException {
        if (backend == null) {
            throw new IOException("stream is closed");
        }
        long pos = backend.sync();
    }
    
    @Override
    public long seek(long marker) throws IOException {
        assert(readOpsAllowed);

        if (backend == null) {
            throw new IOException("stream is closed");
        }    
        
        backend.seek(marker);
        return marker;
    }
    
    @Override
    public Chunk read(Direction dir) throws IOException { 
        assert(readOpsAllowed);
        
        if (backend == null) {
            throw new IOException("stream is closed");
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
            lock.channel().close();
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
            throw new IOException("DB home " + directory.getAbsolutePath() + " does not exist.");
        }
        
        backend = new NIOStreamImpl(directory, segmentSize, memorySize);

        lockFile = new File(directory, "FRS.lck");
        boolean crashed = !lockFile.createNewFile();
        
        FileOutputStream w = new FileOutputStream(lockFile);
        FileChannel lastSync = w.getChannel();
        lock = lastSync.tryLock();
        if (lock == null) {
            throw new IOException(LOCKFILE_ACTIVE);
        }

        backupLockFile = new File(directory, NIOConstants.BACKUP_LOCKFILE);
        backupLockFile.createNewFile();

        backend.open();
    }

    @Override
    public String toString() {
        return "NIO - " + directory.getAbsolutePath();
    }
    
    public boolean isClosed() {
        return !(lock != null && lock.isValid());
    }

    @Override
    public synchronized IOStatistics getStatistics() throws IOException {
        if (backend == null) {
            throw new IOException("stream is closed");
        }
        
        readOpsAllowed = false;
        try {
            return new NIOStatistics(directory, backend.getTotalSize(), backend.findLogTail(), written, read);
        } finally {
            readOpsAllowed = true;
        }
    }
    
    @Override
    public synchronized Future<Void> clean(long timeout) throws IOException {
        if (snapshots > 0) {
            LOGGER.debug("Live snapshots are still around. Delaying cleaning until all snapshots are released.");
            return NullFuture.INSTANCE;
        }

        if (backend == null) {
            throw new IOException("stream is closed");
        }
        
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug("PRE-clean " + this.getStatistics());
        }
        
        readOpsAllowed = false;
        FileOutputStream fos = new FileOutputStream(backupLockFile);
        FileChannel channel = fos.getChannel();
        FileLock backupLock = null;
        try {
          backupLock = channel.tryLock(0, Long.MAX_VALUE, false);
        } catch (OverlappingFileLockException e) {
          LOGGER.info("Backup file already locked.");
        }
        try {
            if (backupLock == null || !backupLock.isValid()) {
                LOGGER.info("Unable to lock backup lockfile. Delaying log file cleanup until the backup is complete.");
                return NullFuture.INSTANCE;
            }
            synchronized (backupLockFile.getCanonicalPath().intern()) {
              backend.trimLogTail(timeout);
            }
        } finally {
            if (backupLock != null) {
                backupLock.release();
            }
            fos.close();
            readOpsAllowed = true;
        }
        
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug("POST-clean " + this.getStatistics());
        }
        return NullFuture.INSTANCE;
    }

    @Override
    public void closeCurrentSegment() throws IOException {
        backend.closeCurrentSegment();
    }

    @Override
    public synchronized Snapshot snapshot() {
        return new NIOSnapshot();
    }

    private class NIOSnapshot implements Snapshot {
        private boolean live = true;
        private final Iterator<File> iterator;

        NIOSnapshot() {
          snapshots++;
          iterator = backend.fileList().iterator();
        }

        @Override
        public boolean hasNext() {
          if (!live) {
            throw new IllegalStateException("snapshot is already released");
          }
          return iterator.hasNext();
        }

        @Override
        public File next() {
          if (!live) {
            throw new IllegalStateException("snapshot is already released");
          }
          return iterator.next();
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("Remove is not supported");
        }

        @Override
        public void close() throws IOException {
          if (live) {
            live = false;
            synchronized (NIOManager.this) {
              snapshots--;
            }
          }
        }
    }
}
