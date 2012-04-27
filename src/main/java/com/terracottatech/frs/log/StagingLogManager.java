/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
        RECOVERY,NORMAL,SHUTDOWN,ERROR,IDLE
    }
    
    private StagingLogManager.IODaemon daemon;
    private volatile CommitList currentRegion;
    private final AtomicLong currentLsn = new AtomicLong(100);
    private final AtomicLong highestOnDisk = new AtomicLong(99);
    private final Signature  checksumStyle;
    private final IOManager io;
    private volatile MachineState   state = MachineState.RECOVERY;
    
    private static final int MAX_QUEUE_SIZE = 1000;
    
    private ChunkExchange exchanger;
    private final ArrayBlockingQueue<LogRegionPacker> queue = new ArrayBlockingQueue<LogRegionPacker>(20);
    
    public StagingLogManager(IOManager io) {
        this(Signature.NONE,new AtomicCommitList( 100l, MAX_QUEUE_SIZE),io);
    }

    public StagingLogManager(Signature check, CommitList list, IOManager io) {
        this.currentRegion = list;
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        this.checksumStyle = check;
    }
    
    
    private synchronized void enterNormalState(long lastLsn) {
        if ( state != MachineState.RECOVERY ) return;
        currentLsn.set(lastLsn + 1);
        highestOnDisk.set(lastLsn);
        currentRegion = currentRegion.create(lastLsn + 1);
        if ( state == MachineState.RECOVERY ) state = MachineState.NORMAL;  
        this.notifyAll();
    }
    
    private synchronized void waitForNormalState() throws InterruptedException {
        while ( state == MachineState.RECOVERY ) {
            this.wait();
        }
    }
        
    private class WriteQueuer extends Thread {
      long waiting;
      long processing;
      List<LogRegionPacker> pool = Collections.synchronizedList(new ArrayList<LogRegionPacker>());
        
      WriteQueuer() {
        setDaemon(true);
        setName("WriteQueuer - Chunk prep and queue for write");
        setPriority(MAX_PRIORITY);
      }
      
      void returnPacker(LogRegionPacker packer) {
          if ( packer == null ) return;
          packer.clear();
          pool.add(packer);
      }
      
      void done() {
          pool = null;
          this.interrupt();
      }
    
      @Override
      public void run() {
        long last = System.nanoTime();
        long turns = 0;
        long size = 0;
        int fill = 0;
        while (pool != null) {
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
            LogRegionPacker packer = ( pool.isEmpty() ) ? new LogRegionPacker(checksumStyle) : pool.remove(0);
            Chunk send = packer.pack(oldRegion);
            queue.put(packer);
            size += queue.size();
            fill += (int)(oldRegion.getEndLsn() - oldRegion.getBaseLsn());
            turns+=1;
         } catch (InterruptedException ie) {
            if ( state == MachineState.NORMAL ) throw new AssertionError(ie);
          } 
        }
        if ( turns == 0 ) turns = 1;
        System.out.format("processing -- waiting: %d active: %d ave. queue: %d fill: %d\n",waiting,processing,size/(turns),fill/turns);
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
          
        exchanger.recover();
        
        try {
          io.seek(IOManager.Seek.END.getValue());
        } catch ( IOException ioe ) {
          throw new AssertionError(ioe);
        }    
        
        try {
            enterNormalState(exchanger.getLastLsn());
        } catch ( InterruptedException ioe ) {
          throw new AssertionError(ioe);
        }  
        
        WriteQueuer queuer = new WriteQueuer();
        queuer.start();     
        
        long last = System.nanoTime();
        while ((state == MachineState.NORMAL || currentLsn.get() - 1 != highestOnDisk.get())) {
          LogRegionPacker packer = null;
          try {
            long mark = System.nanoTime();
            writing += (mark - last);
            
            packer = queue.poll(100,TimeUnit.MILLISECONDS);
            last = System.nanoTime();
            waiting += (last - mark);

            if ( packer == null ) {
                continue;
            }
           
            long out = io.write(packer.take());

            if ( packer.doSync() ) {
              io.sync();
            }
            
            highestOnDisk.set(packer.lsn());
            packer.written();
          } catch (IOException ioe) {
            throw new AssertionError(ioe);
          } catch (InterruptedException ie) {
            if ( state == MachineState.NORMAL ) throw new AssertionError(ie);
          } finally {
              queuer.returnPacker(packer);
          }
        }
        
        try {
            queuer.done();
            queuer.join();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        
        state = MachineState.IDLE;
        System.out.format("write -- waiting: %d active: %d\n",waiting,writing);

      }
    }

    //  TODO:  re-examine when more runtime context is available.
    public void startup() {
        state = MachineState.RECOVERY;
        exchanger = new ChunkExchange(io, checksumStyle,MAX_QUEUE_SIZE);
        this.daemon = new IODaemon();
        this.daemon.start();
        
    }

    //  TODO:  re-examine when more runtime context is available.
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
            io.close();
        } catch ( IOException ioe ) {
            throw new AssertionError(ioe);
        }
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
        
        long lsn = currentLsn.getAndIncrement();
        try {
            record.updateLsn(lsn);
        } finally {
        
   // if highest lsn on disk is < the lowest lsn transmitted by record, then 
   // our entire log stream on disk is old.  We also don't know if ObjectManager 
   // has all lsn's committed back to it from logmanager so set record lowest lsn 
   // to the value we know is confirmed back to ObjectManager
            long highOnDisk = highestOnDisk.get();
            if ( record.getLowestLsn() > highOnDisk ) {
                record.setLowestLsn(highOnDisk);
            }

            CommitList mine = currentRegion;

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

            return mine;
        }
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
        Iterator<LogRecord> it = exchanger.iterator();
        it.hasNext();

        return it;
    }
        
    public ChunkExchange getRecoveryExchanger() {
        return exchanger;
    }


}
