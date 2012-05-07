/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.RotatingBufferSource;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Simple LogManager with a single daemon thread for IO operations
 * 
 * 
 * @author mscott
 */
public class StagingLogManager implements LogManager {
        
    static enum MachineState {
        BOOTSTRAP,NORMAL,SHUTDOWN,ERROR,IDLE
    }
    
    private StagingLogManager.IODaemon daemon;
    private volatile CommitList currentRegion;
    private final AtomicLong currentLsn = new AtomicLong(100);
    private final AtomicLong highestOnDisk = new AtomicLong(99);
    private final Signature  checksumStyle;
    private final IOManager io;
    private volatile MachineState   state = MachineState.IDLE;
    
    private static final int MAX_QUEUE_SIZE = 1000;
    
    private ChunkExchange           exchanger;
    private final ArrayBlockingQueue<WritingPackage> queue = new ArrayBlockingQueue<WritingPackage>(20);
    
    public StagingLogManager(IOManager io) {
        this(Signature.NONE,new AtomicCommitList( 100l, MAX_QUEUE_SIZE),io);
    }

    public StagingLogManager(Signature check, CommitList list, IOManager io) {
        this.currentRegion = list;
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        this.checksumStyle = check;
    }

    @Override
    public long currentLsn() {
      return currentLsn.get();
    }
    
 
    private synchronized void enterNormalState(long lastLsn) {
        if ( state != MachineState.BOOTSTRAP ) return;
        currentLsn.set(lastLsn + 1);
        highestOnDisk.set(lastLsn);
        currentRegion = currentRegion.create(lastLsn + 1);
        if ( state == MachineState.BOOTSTRAP ) state = MachineState.NORMAL;  
        this.notifyAll();
    }   
    
    private synchronized void waitForNormalState() throws InterruptedException {
        while ( state == MachineState.BOOTSTRAP ) {
            this.wait();
        }
    }
        
    private class WriteQueuer extends Thread {
      long waiting;
      long processing;
      
      RotatingBufferSource    buffers = new RotatingBufferSource();
      LogRegionFactory        regionFactory = new CopyingPacker(checksumStyle,buffers);
//      LogRegionFactory        regionFactory = new LogRegionPacker(checksumStyle);
      
        
      WriteQueuer() {
        setDaemon(true);
        setName("WriteQueuer - Chunk prep and queue for write");
        setPriority(MAX_PRIORITY);
      }
      
      void done() {
          regionFactory = null;
          this.interrupt();
      }
    
      @Override
      public void run() {
        long last = System.nanoTime();
        long turns = 0;
        long size = 0;
        int fill = 0;
        while (regionFactory != null) {
          CommitList oldRegion = currentRegion;
          try {
            if ( state != MachineState.NORMAL ) {
                if (currentLsn.get()-1 >= oldRegion.getBaseLsn() ) {
                    oldRegion.close(currentLsn.get()-1);
                }
            }
            long mark = System.nanoTime();
            processing += (mark - last);
            oldRegion.waitForContiguous();
            last = System.nanoTime();
            waiting += (last - mark);
            last = System.nanoTime();
            currentRegion = oldRegion.next();
            
            if ( oldRegion.isEmpty() ) {
                oldRegion.written();
                continue;
            }
            queue.put(new WritingPackage(oldRegion,regionFactory.pack(oldRegion)));
            size += queue.size();
            fill += (int)(oldRegion.getEndLsn() - oldRegion.getBaseLsn());
            turns+=1;
         } catch (InterruptedException ie) {
            if ( state == MachineState.NORMAL ) throw new AssertionError(ie);
          } 
        }
        if ( turns == 0 ) turns = 1;
        System.out.format("processing -- waiting: %.3f active: %.3f ave. queue: %d fill: %d\n",waiting*1e-6,processing*1e-6,size/(turns),fill/turns);
      }
    }

    private class IODaemon extends Thread {
    long waiting;
    long writing;
    
      IODaemon() {
        setDaemon(true);
        setName("IO - All IO Here");
//        setPriority(MAX_PRIORITY);
      }

      @Override
      public void run() {
        WriteQueuer queuer = new WriteQueuer();
        queuer.start();  
        
        long lowestLsn = 0;
        
        long last = System.nanoTime();
        int cleanCount = 0;
        long lastClean = 0;
        while ((state == MachineState.NORMAL || currentLsn.get() - 1 != highestOnDisk.get())) {
          WritingPackage packer = null;
          try {
            long mark = System.nanoTime();
            writing += (mark - last);
            
            packer = queue.poll(1000,TimeUnit.MILLISECONDS);
            last = System.nanoTime();
            waiting += (last - mark);

            if ( packer == null ) {
                if ( cleanCount++ > 10 && io.getMinimumMarker() - lastClean > 10000 ) {
                    io.clean(0);
                    lastClean = io.getMinimumMarker();
                    cleanCount = 0;
                }
                continue;
            }
           
            if ( packer.getLowestLsn() > lowestLsn ) {
                io.setMinimumMarker(packer.getLowestLsn());
                lowestLsn = packer.getLowestLsn();
            }
            io.setCurrentMarker(packer.baseLsn());
            io.setMaximumMarker(packer.endLsn());
            long out = io.write(packer.take());

            if ( packer.doSync() ) {
              io.sync();
            }
            
            highestOnDisk.set(packer.endLsn());
            packer.written();
          } catch (IOException ioe) {
            throw new AssertionError(ioe);
          } catch (InterruptedException ie) {
            if ( state == MachineState.NORMAL ) throw new AssertionError(ie);
          } finally {

          }
        }
        
        try {
            queuer.done();
            queuer.join();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        
        state = MachineState.IDLE;
        System.out.format("write -- waiting: %.3f active: %.3f\n",waiting*1e-6,writing*1e-6);

      }
    }
    
    public Future<Void> recover() {
        if ( exchanger != null ) return exchanger;
        
        exchanger = new ChunkExchange(io, checksumStyle,MAX_QUEUE_SIZE);
        
        exchanger.recover();
        
        return exchanger;
    }

    //  TODO:  re-examine when more runtime context is available.
    @Override
    public void startup() {
        assert(MachineState.IDLE == state);
        
        state = MachineState.BOOTSTRAP;
        
        if ( exchanger == null ) recover();
        
        try {
            enterNormalState(exchanger.getLastLsn());
        } catch ( InterruptedException ioe ) {
          throw new AssertionError(ioe);
        }  
        
        this.daemon = new IODaemon();
        this.daemon.start();
        
    }

    //  TODO:  re-examine when more runtime context is available.
    @Override
    public void shutdown() {
        state = MachineState.SHUTDOWN;
        
        CommitList  current = currentRegion;

        current.close(currentLsn.get()-1);
        
        try {
            daemon.join();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        if (daemon.isAlive()) {
            throw new AssertionError();
        }
        if (currentLsn.get()-1 != highestOnDisk.get()) {
            throw new AssertionError();
        }
        try {
            exchanger.cancel(true);
            exchanger.get();
        } catch ( ExecutionException ee ) {
            
        } catch ( InterruptedException ie ) {
            
        }
        try {
            io.close();
        } catch ( IOException ioe ) {
            throw new AssertionError(ioe);
        }
        exchanger = null;
        state = MachineState.IDLE;
    }
    
    private CommitList _append(LogRecord record, boolean sync) {
        if ( state == MachineState.SHUTDOWN ) {
            throw new RuntimeException("shutting down");
        }
        
        try {
            waitForNormalState();
        } catch ( InterruptedException it ) {
            throw new RuntimeException(it);
        }
        CommitList mine = currentRegion;
        long lsn = currentLsn.getAndIncrement();
        try {
            record.updateLsn(lsn);
        } catch ( Error e ) {
            throw e;
        } finally {
        
   // if highest lsn on disk is < the lowest lsn transmitted by record, then 
   // our entire log stream on disk is old.  We also don't know if ObjectManager 
   // has all lsn's committed back to it from logmanager so set record lowest lsn 
   // to the value we know is confirmed back to ObjectManager
            long highOnDisk = highestOnDisk.get();
            if ( record.getLowestLsn() > highOnDisk ) {
                record.setLowestLsn(highOnDisk);
            }

            int spincount = 0;
            while ( !mine.append(record,sync) ) {
    //            mine.close(record.getLsn(),false);   // this is not needed
                if ( spincount++ > 10 ) {
                    try {
                        mine.get();
                    } catch ( InterruptedException ie ) {

                    } catch ( ExecutionException ee ) {

                    }
                }
                if ( mine.getEndLsn() >= mine.getBaseLsn() ) {

                }
                mine = mine.next();

            }

        }
        return mine;
    }


    @Override
    public CommitList append(LogRecord record) {
        return _append(record,false);
    }
    
    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        return _append(record,true);
    }

    @Override
    public Iterator<LogRecord> reader() {
        if ( exchanger == null ) this.startup();
        
        Iterator<LogRecord> it = exchanger.iterator();
        it.hasNext();

        return it;
    }
        
    public ChunkExchange getRecoveryExchanger() {
        return exchanger;
    }


    static class WritingPackage {
        CommitList list;
        Chunk      data;
        
        WritingPackage(CommitList list, Chunk data) {
            this.list= list;
            this.data = data;
        }
        
        public long getLowestLsn() {
            return list.getLowestLsn();
        }
    
        public long endLsn() {
            return list.getEndLsn();
        }
    
        public long baseLsn() {
            return list.getBaseLsn();
        }
        
        public boolean doSync() {
            return list.isSyncRequested();
        }

        public void written() {
            list.written();
        }
        
        public Chunk take() {
            try {
                return data;
            } finally {
                data = null;
            }
        }
    }
}
