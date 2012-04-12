/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.IOManager;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Simple LogManager with a single daemon thread for IO operations
 * 
 * 
 * @author mscott
 */
public class SimpleLogManager implements LogManager,Runnable {
    
    private final Thread daemon;
    private volatile CommitList currentRegion;
    private final AtomicLong currentLsn = new AtomicLong(100);
    private final AtomicLong highestOnDisk = new AtomicLong(0);
    private final Signature  checksumStyle = Signature.ADLER32;
    private final IOManager io;
    private volatile boolean alive = true;
    private long totalBytes = 0;
    
    private LogRegionPacker  packer;   
    
    public SimpleLogManager(IOManager io) {
        this(new AtomicCommitList(true, 100l, 100),io);
    }

    public SimpleLogManager(CommitList list, IOManager io) {
        this.currentRegion = list;
        this.daemon = new Thread(this);
        this.daemon.setDaemon(true);
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        packer = new LogRegionPacker(checksumStyle);   
    }
    
     //  TODO:  re-examine when more runtime context is available.
    @Override
    public void run() {
        while ( alive || currentLsn.get()-1 != highestOnDisk.get() ) {
            CommitList old_region = currentRegion;
            try {
                if ( !alive ) old_region.close(currentLsn.get() -1,false);
                old_region.waitForContiguous();
                
                currentRegion = old_region.next();
                
                totalBytes += io.write(packer.pack(old_region));
                if ( old_region.isSyncing() ) {
                    io.sync();
                }
                highestOnDisk.set(old_region.getEndLsn());
                old_region.written();
            } catch ( IOException ioe ) {
                throw new AssertionError(ioe);
            } catch ( InterruptedException ie ) {
                if ( alive ) throw new AssertionError(ie);
                else old_region.close(currentLsn.get()-1,false);
            }        
        }
    }
    //  TODO:  re-examine when more runtime context is available.
    public void startup() {
        this.daemon.start();
    }

    //  TODO:  re-examine when more runtime context is available.
    public void shutdown() {
        alive = false;
        currentRegion.close(currentLsn.get()-1,false);
        try {
            daemon.join();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        assert(currentLsn.get()-1 == highestOnDisk.get());
        try {
            io.close();
        } catch ( IOException ioe ) {
            throw new AssertionError(ioe);
        }
    }
    
    public long totalBytes() {
        return totalBytes;
    }
    
    private CommitList commonAppend(LogRecord record) {
//        if ( !alive.get() ) throw new RuntimeException("shutting down");
        long lsn = currentLsn.getAndIncrement();
        record.updateLsn(lsn);
        
   // if highest lsn on disk is < the lowest lsn transmitted by record, then 
   // our entire log stream on disk is old.  We also don't know if ObjectManager 
   // has all lsn's committed back to it from logmanager so set record lowest lsn 
   // to the value we know is confirmed back to ObjectManager
        long highOnDisk = highestOnDisk.get();
        if ( record.getLowestLsn() > highOnDisk ) {
            record.setLowestLsn(highOnDisk);
        }
        
        CommitList mine = currentRegion;
                
        while ( !mine.append(record) ) {
            mine.close(record.getLsn(),false);
            mine = mine.next();
        }
        
        return mine;
    }
    
    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        final CommitList mine = commonAppend(record);
        
        mine.close(record.getLsn(),true);
                
        return mine;
    }

    @Override
    public Future<Void> append(LogRecord record) {
        
        CommitList mine = commonAppend(record);
                
        return mine;
    }

    @Override
    public Iterator<LogRecord> reader() {
        ChunkExchange ex = new ChunkExchange(io, checksumStyle);
        Thread reader = new Thread(ex);
        reader.setDaemon(true);
        reader.start();
        return ex.iterator();
    }

}
