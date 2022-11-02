/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.BufferBuilder;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SLABBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.util.NullFuture;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
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
    private final boolean       randomAccess;
    private final long          memorySize;
    private final long          randomAccessSize;
    private boolean       disableSync;
    private boolean             useSlabs = false;
        
    private static final String LOCKFILE_ACTIVE = "lock file exists";
    
    private NIOStreamImpl backend;
    private NIORandomAccess  reader;
    private BufferSource  mainBuffers;
    private long written = 0;
    private long read = 0;
    private long writeTime = 0;
    private long parts = 0;
    private long requests = 0;

    private int snapshots = 0;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(IOManager.class);
     
    public NIOManager(String home, String method, String memoryType, long segmentSize, long memorySize, long randomAccessSize, boolean randomAccess, BufferSource src) throws IOException {
        directory = new File(home);
                
        this.segmentSize = segmentSize;
        
        this.memorySize = memorySize;
        
        this.randomAccessSize = randomAccessSize;
        
        this.randomAccess = randomAccess;
        
        this.mainBuffers = src;
        
        this.useSlabs = memoryType != null && memoryType.equals("SLAB");
        
        open(NIOAccessMethod.valueOf(method));
    }
    
    public NIOManager(Configuration config, BufferSource writer) throws IOException {
        this(config.getDBHome().getAbsolutePath(),
            config.getString(FrsProperty.IO_NIO_ACCESS_METHOD),
            config.getString(FrsProperty.IO_NIO_BUFFER_SOURCE),
            config.getLong(FrsProperty.IO_NIO_SEGMENT_SIZE),
            config.getLong(FrsProperty.IO_NIO_RECOVERY_MEMORY_SIZE),
            config.getLong(FrsProperty.IO_NIO_RANDOM_ACCESS_MEMORY_SIZE),
            config.getBoolean(FrsProperty.IO_RANDOM_ACCESS),
            writer
        );

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
        if ( randomAccess ) {
          reader.setMaxFiles(config.getInt(FrsProperty.IO_NIO_FILECACHE_MAX));
        }
        
        if ( config.getBoolean(FrsProperty.IO_DISABLE_SYNC) ) {
          this.backend.disableSync(true);
        }
    }
// for tests
    void setBufferBuilder(BufferBuilder builder) {
        if ( backend != null ) {
            backend.setBufferBuilder(builder);
        }
    }

    @Override
    public long write(Chunk region, long marker) throws IOException {
        if (backend == null) {
            throw new IOException("stream is closed");
        }    
        
        long blit = System.nanoTime();
        long w = backend.append(region, marker);
        if ( region instanceof SnapshotRequest ) {
            ((SnapshotRequest)region).setSnapshot(new NIOSnapshot());
        }
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
        if (backend == null) {
            throw new IOException("stream is closed");
        }    
        
        backend.seek(marker);
        return marker;
    }
    
    private BufferSource getRandomAccessBufferSource() {
      if ( randomAccessSize < 0 ) {
        return mainBuffers;
      } else {
        return new MaskingBufferSource(( useSlabs ) ? 
            new SLABBufferSource((int)this.randomAccessSize) : 
            new SplittingBufferSource(64,(int)this.randomAccessSize));
      }
    }
    
    private BufferSource getRecoveryBufferSource(NIOAccessMethod method) {
      return ( this.memorySize < 0 || method != NIOAccessMethod.STREAM ) ? mainBuffers :
        new MaskingBufferSource(( useSlabs ) ? 
            new SLABBufferSource((int)this.memorySize) : 
            new SplittingBufferSource(64, (int)this.memorySize));
    }
    
    @Override
    public Chunk scan(long marker) throws IOException {
        if (backend == null) {
            throw new IOException("stream is closed");
        }    
        try {
            boolean waited = this.backend.waitForWriteOf(marker);
            if ( reader == null ) {
                this.reader = backend.createRandomAccess(getRandomAccessBufferSource());
            }
            Chunk c = this.reader.scan(marker);
            if ( c == null ) {
                throw new AssertionError("Marker " + marker + ":" + this.backend.getMarker() + " not found in " + directory + " during scan; waited:" + waited);
                }
            return c;
        } catch ( InterruptedIOException ioe ) {
            Thread.currentThread().interrupt();
            if ( this.isClosed() ) {
                throw new IllegalStateException("closed during get operation");
            } else {
                throw new InterruptedIOException("random access interrupted");
            }
        } catch ( InterruptedException ie ) {
            Thread.currentThread().interrupt();
            if ( this.isClosed() ) {
                throw new IllegalStateException("closed during get operation");
            } else {
                throw new InterruptedIOException("random access interrupted");
            }
        }
    }
    
    @Override
    public Chunk read(Direction dir) throws IOException {         
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
        mainBuffers.reclaim();
    }
    
    File getHomeDirectory() {
      return directory;
    }
    
    private void open(NIOAccessMethod method) throws IOException {        
        if (!directory.exists() || !directory.isDirectory()) {
            throw new IOException("DB home " + directory.getAbsolutePath() + " does not exist.");
        }
        LOGGER.info("opening with " + method + " access method");
        backend = new NIOStreamImpl(directory, method, segmentSize, this.mainBuffers, this.getRecoveryBufferSource(method));

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
        
        if ( randomAccess ) {
            reader = backend.createRandomAccess(getRandomAccessBufferSource());
        }
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
        
        return new LiveNIOStatistics(directory, backend, written, read);
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
                LOGGER.info("Unable to lock backup lockfile. Delaying log file cleanup until the backup is " +
                  "complete.");
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
        }
        
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug("POST-clean " + this.getStatistics());
        }
        return NullFuture.INSTANCE;
    }

    private class NIOSnapshot implements Snapshot {
        private boolean live = true;
        private final List<File> files;

        NIOSnapshot() {
          snapshots++;
          files = Collections.unmodifiableList(backend.fileList());
        }

        @Override
        public Iterator<File> iterator() {
            return files.iterator();
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
