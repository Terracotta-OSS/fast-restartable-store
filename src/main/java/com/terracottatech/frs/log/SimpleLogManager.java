/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.Seek;
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
public class SimpleLogManager implements LogManager {
    
    private LogWriter daemon;
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
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        packer = new LogRegionPacker(checksumStyle);   
    }

    private class LogWriter extends Thread {
      LogWriter() {
        setDaemon(true);
        setName("LogManager - Writer Thread");
      }

      @Override
      public void run() {
        try {
          io.seek(Seek.END.getValue());
        } catch ( IOException ioe ) {
          throw new AssertionError(ioe);
        }

        while ((alive || currentLsn.get() - 1 != highestOnDisk.get())) {
          CommitList oldRegion = currentRegion;
          try {
            if ( !alive ) {
              CommitList closeAll = oldRegion;
              while (!closeAll.close(currentLsn.get() - 1, true)) {
                closeAll = closeAll.next();
              }
            }

            oldRegion.waitForContiguous();

            currentRegion = oldRegion.next();

            totalBytes += io.write(packer.pack(oldRegion));
            if ( oldRegion.isSyncRequested() ) {
              io.sync();
            }
            highestOnDisk.set(oldRegion.getEndLsn());
            oldRegion.written();
          } catch (IOException ioe) {
            throw new AssertionError(ioe);
          } catch (InterruptedException ie) {
            if ( alive ) throw new AssertionError(ie);
            else oldRegion.close(currentLsn.get()-1,true);
          }
        }
      }
    }

    //  TODO:  re-examine when more runtime context is available.
    public void startup() {
        try {
            io.open();
        } catch ( IOException ioe ) {
            ioe.printStackTrace();
        }
        if ( exchanger != null ) this.currentLsn.set(exchanger.getLasLsn() + 1);
        if ( exchanger != null ) this.currentRegion.setBaseLsn(exchanger.getLasLsn() + 1);
        this.alive = true;
        this.daemon = new LogWriter();
        this.daemon.start();
    }

    //  TODO:  re-examine when more runtime context is available.
    public void shutdown() {
        alive = false;
        CommitList  current = currentRegion;
        
        while ( !current.close(currentLsn.get()-1,false) ) {
            current = current.next();
        }
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

    @Override
    public CommitList append(LogRecord record) {
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
//            mine.close(record.getLsn(),false);   // this is not needed
            mine = mine.next();
        }
        
        return mine;
    }
    
    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        final CommitList mine = append(record);
        
        if ( !mine.close(record.getLsn(),true) ) {
   //  this close has to be in the range, it just got set
            throw new AssertionError();
        }
                
        return mine;
    }

    @Override
    public Iterator<LogRecord> reader() {
        exchanger = new ChunkExchange(io, checksumStyle);
        Thread reader = new Thread(exchanger);
        reader.setDaemon(true);
        reader.start();
        return exchanger.iterator();
    }
    
    public ChunkExchange exchanger;
    
    public ChunkExchange getRecoveryExchanger() {
        return exchanger;
    }

}
