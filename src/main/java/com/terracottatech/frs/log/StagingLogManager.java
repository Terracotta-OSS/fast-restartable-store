/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.ManualBufferSource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Simple LogManager with a single daemon thread for IO operations
 * 
 * 
 * @author mscott
 */
public class StagingLogManager implements LogManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    private StagingLogManager.IODaemon daemon;
    private volatile CommitList currentRegion;
    private final AtomicLong currentLsn = new AtomicLong(100);
    private final AtomicLong lowestLsn = new AtomicLong(0);
    private final AtomicLong highestOnDisk = new AtomicLong(99);
    private Signature  checksumStyle;
    private final IOManager io;
    private volatile LogMachineState   state = LogMachineState.IDLE;
    
    private int MAX_QUEUE_SIZE;
    private int RECOVERY_QUEUE_SIZE = 64;
    
    private ChunkExchange                               exchanger;
    private final BlockingQueue<WritingPackage>         queue = new ArrayBlockingQueue<WritingPackage>(8);
    private IOException                                 blockingException;
    
    private BufferSource    buffers = new ManualBufferSource(100 * 1024 * 1024);
    
    public StagingLogManager(IOManager io) {
        this(Signature.ADLER32,new AtomicCommitList( 100l, 1024, 200),io);
    }
        
    public StagingLogManager(IOManager io,Configuration config) {
        this(Signature.ADLER32,new AtomicCommitList( 100l, 1024, 200),io);
        String checksum = config.getString(FrsProperty.IO_CHECKSUM);
        this.checksumStyle = Signature.valueOf(checksum);
        this.MAX_QUEUE_SIZE = config.getInt(FrsProperty.IO_COMMIT_QUEUE_SIZE);
        this.RECOVERY_QUEUE_SIZE = config.getInt(FrsProperty.IO_RECOVERY_QUEUE_SIZE);
        String commitList = config.getString(FrsProperty.IO_COMMITLIST);
        if ( commitList.equals("ATOMIC") ) {
            this.currentRegion = new AtomicCommitList(100, MAX_QUEUE_SIZE, config.getInt(FrsProperty.IO_WAIT));
        } else if ( commitList.equals("STACKING") ) {
            this.currentRegion = new StackingCommitList(100, MAX_QUEUE_SIZE, config.getInt(FrsProperty.IO_WAIT));
        }
    }

    public StagingLogManager(Signature check, CommitList list, IOManager io) {
        this.currentRegion = list;
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        this.checksumStyle = check;
        this.MAX_QUEUE_SIZE = 1024;
    }

    @Override
    public long currentLsn() { 
      return currentLsn.get();
    }
    
    long firstCommitListLsn() {
        return currentRegion.getBaseLsn();
    }

    @Override
    public void updateLowestLsn(long lsn) {
        long cl = lowestLsn.get();
        long onDisk = highestOnDisk.get();

        if ( exchanger ==null || !exchanger.isDone() ) {
            throw new AssertionError("cannot update lowest lsn until recovery is finished");
        }
        
        if ( !state.acceptRecords() ) return;
        
        if ( lsn > onDisk ) {
 //  highest on disk is lower than lowest, entire log on disk is old, set lowestLsn to the highest 
 //  currently on disk.
            lsn = onDisk;
        }
        if ( lsn > cl ) {
            try {
                if ( lowestLsn.compareAndSet(cl, lsn) ) {
                    io.setMinimumMarker(lsn);
                    io.clean(0);
                }
            } catch ( ClosedByInterruptException in ) {
     //  someone interrupted the thread, just return
            } catch ( IOException ioe ) {
                throw new RuntimeException(ioe);
            }
        }
    }

    @Override
    public long lowestLsn() {
        return lowestLsn.get();
    }

    private synchronized void enterNormalState(long lastLsn, long lowest) {
        if ( !state.isBootstrapping() ) return;
        
        currentLsn.set(lastLsn + 1);
        highestOnDisk.set(lastLsn);
        if ( lowest < 100 ) lowest = 100;
        lowestLsn.set(lowest);
        
        currentRegion = currentRegion.create(lastLsn + 1);

        state = state.progress();
                
        this.notifyAll();
    }   
    
    private synchronized void waitForNormalState() throws InterruptedException {
        while ( state.starting() ) {
            this.wait();
        }
        if ( !state.acceptRecords() ) {
            throw new RuntimeException("normal state not achieved");
        }
    }
        
    private class WriteQueuer extends Thread {
      long waiting;
      long processing;
      
      volatile boolean        stopped = false;
      private final LogRegionFactory  regionFactory = new CopyingPacker(checksumStyle,buffers);
      private final ExecutorService   asyncPacker = Executors.newCachedThreadPool(new ThreadFactory() {

            int count = 1;
          
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("async packing thread - " + count++);
                t.setDaemon(true);
                return t;
            }
        });
      
      WriteQueuer() {
        setDaemon(true);
        setName("Write Queue Manager - " + io.toString());
        setPriority(MAX_PRIORITY);
      }
      
      void done() {
          stopped = true;
          this.interrupt();
      }
    
      @Override
      public void run() {
        long last = System.nanoTime();
        long turns = 0;
        long size = 0;
        int fill = 0;
        try {
        while (!stopped) {
          CommitList oldRegion = currentRegion;
          try {
            if ( !state.acceptRecords() && currentLsn.get()-1 >= oldRegion.getBaseLsn() ) {
                oldRegion.close(currentLsn.get()-1);
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
     //  only skip the IO queue if records are still being accepted,
     //  if not, the IO thread may need to loosened to shutdown
                if ( state.acceptRecords() ) continue;
            }
            
            WritingPackage wp = new WritingPackage(oldRegion,regionFactory);
            wp.run();

            while ( !queue.offer(wp,200,TimeUnit.MICROSECONDS) ) {
                if ( stopped ) break;
            }
            
            size += queue.size();
            int lf = (int)(oldRegion.getEndLsn() - oldRegion.getBaseLsn());
            fill += lf;
            turns+=1;
//  if in synchronous mode, wait here so more log records are batched in the next region
            if ( state.acceptRecords() && oldRegion.isSyncRequested() ) {
                try {
                    oldRegion.get();
                } catch ( ExecutionException ee ) {
                    //  ignore this, this wait is a performace optimization
                }
            }
          } catch (InterruptedException ie) {
            state.checkException(ie);
          }
        }
        
        asyncPacker.shutdown();
        if ( turns == 0 ) turns = 1;
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(processing)== waiting: %.3f active: %.3f ave. queue: %d fill: %d",
                    waiting*1e-6,processing*1e-6,size/(turns),fill/turns).out().toString());
        }
        } catch ( OutOfMemoryError oome ) {
            LOGGER.error("on write queue thread",oome);
        }
      }
    }

    private class IODaemon extends Thread {
    long waiting;
    long writing;
    long written;
    
      IODaemon() {
        setDaemon(true);
        setName("IO - " + io.toString());
        setPriority(MAX_PRIORITY);
      }

      @Override
      public void run() {
        WriteQueuer queuer = new WriteQueuer();
        queuer.start();  
                
        long last = System.nanoTime();
        while ( state.acceptRecords() || currentLsn.get() - 1 != highestOnDisk.get()) {
          WritingPackage packer = null;
          try {
            long mark = System.nanoTime();
            writing += (mark - last);
            
            packer = queue.poll(1000,TimeUnit.MILLISECONDS);
            last = System.nanoTime();
            waiting += (last - mark);

            if ( packer == null ) {
                continue;
            }
           
            Chunk c = packer.take();
            if (io.getCurrentMarker()+1 != packer.baseLsn()) {
                throw new AssertionError("lsns not sequenced");
            }
            written += io.write(c,packer.endLsn());
            for ( ByteBuffer giveBack : c.getBuffers() ) {
                buffers.returnBuffer(giveBack);
            }

            if ( packer.doSync() ) {
              io.sync();
            }
            
            highestOnDisk.set(packer.endLsn());
            packer.written();
            
          } catch (IOException ioe) {
            blockingException = ioe;
            state = state.checkException(ioe);
            if ( packer != null ) {
                packer.list.exceptionThrown(ioe);
            }
            break;
          } catch (InterruptedException ie) {
            state = state.checkException(ie);
          } 
        }
        
        try {
            if ( state.isErrorState() ) {
//  clear any items in the queue
                long floatingLsn = highestOnDisk.get();
                while ( currentLsn.get() - 1 != floatingLsn ) {
                    CommitList next = queue.take().list;
                    next.exceptionThrown(blockingException);
                    floatingLsn = next.getEndLsn();
                }
            } else if ( !queue.isEmpty() ) {
                while ( !queue.isEmpty() ) {
                    if ( !queue.poll().list.isEmpty() ) {
                        throw new AssertionError("non-empty queue");
                    }
                }
            }
            queuer.done();
            queuer.join();
        } catch ( InterruptedException ie ) {
            throw new RuntimeException(ie);
        }
        
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(logwrite)== waiting: %.3f active: %.3f written: %d",waiting*1e-6,writing*1e-6,written).out().toString());
        }
      }
    }
    
    private Future<Void> recover() {        
        ChunkExchange ex = new ChunkExchange(io, RECOVERY_QUEUE_SIZE);
        LOGGER.debug("recovery queue size: " + RECOVERY_QUEUE_SIZE);
        
        ex.recover();
        
        return ex;
    }

    //  TODO:  re-examine when more runtime context is available.
    @Override
    public Iterator<LogRecord> startup() {      
        if ( state != LogMachineState.IDLE ) state = state.reset();
        
        state = state.bootstrap();
        
        try {
            if ( state == LogMachineState.BOOTSTRAP ) {
                exchanger = (ChunkExchange)recover();

                try {
                    enterNormalState(exchanger.getLastLsn(), exchanger.getLowestLsn());
                } catch ( InterruptedException ioe ) {
                    throw new RuntimeException(ioe);
                }  

                return exchanger.iterator();
            } else {
                return null;
            }
        } finally {
            this.daemon = new IODaemon();
            this.daemon.start();
        }
        
    }

    //  TODO:  re-examine when more runtime context is available.
    @Override
    public void shutdown() {        
        try {
            state = state.shutdown();
            if ( state != LogMachineState.SHUTDOWN ) {
        //  just log and fail fast
                try {
        //  io should still be closed
                    io.close();
                } catch ( IOException ioe ) {
                    LOGGER.error("error closing io",ioe);
        //  log and continue
                }
                return;
            }
        } catch ( Throwable t ) {
     //  just log and fail fast
            try {
     //  io should still be closed
                io.close();
            } catch ( IOException ioe ) {
                LOGGER.error("error closing io",ioe);
     //  log and continue
            }
            LOGGER.error("was in " + state + " at shutdown",t);
            return;
        }
        
        CommitList  current = currentRegion;

        current.close(currentLsn.get()-1);
        
        try {
            daemon.join();
        } catch ( InterruptedException ie ) {
            LOGGER.error("error waiting for write thread to close",ie);
        }
        if (daemon.isAlive()) {
            throw new AssertionError();
        }
        if (!state.isErrorState() && currentLsn.get()-1 != highestOnDisk.get()) {
            throw new AssertionError();
        }
        
        try {
            exchanger.cancel(true);
            exchanger.get();
        } catch ( ExecutionException ee ) {
            LOGGER.error("error during shutdown",ee);
        } catch ( InterruptedException ie ) {
            LOGGER.error("error during shutdown",ie);
        }
        try {
            io.close();
        } catch ( IOException ioe ) {
//  log io error and close            
            LOGGER.error("error closing io",ioe);
        }
        state = state.idle();
    }
    
    private CommitList _append(LogRecord record, boolean sync) {
        if ( !state.acceptRecords() ) {
            if ( blockingException != null ) {
                throw new LogWriteError(blockingException);
            } else {
                throw new RuntimeException("frs is not accepting records");
            }
        }
        
        while ( state.starting() ) {
            try {
                waitForNormalState();
            } catch ( InterruptedException it ) {
                throw new RuntimeException(it);
            }
        }
        
        CommitList mine = currentRegion;
        long lsn = currentLsn.getAndIncrement();
        try {
            record.updateLsn(lsn);
        } catch ( Error e ) {
            throw e;
        } finally {
       
            int spincount = 0;
  //  if we hit this, try and spread out
            int waitspin = 2 + (Math.round((float)(Math.random() * 1024f)));
            while ( !mine.append(record,sync) ) {
                if ( spincount++ > waitspin ) {
                    try {                      
                        mine.get();
                        waitspin += (Math.round((float)(Math.random() * 512f)));
                    } catch ( InterruptedException ie ) {

                    } catch ( ExecutionException ee ) {

                    }
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
        Future<Void> w = _append(record,true);
        return w;
    }

    static class WritingPackage implements Runnable {
        private final CommitList                list;
        private volatile LogRegionFactory      factory;
        private volatile Chunk                  data;
        
        WritingPackage(CommitList list, LogRegionFactory factory) {
            this.list= list;
            this.factory = factory;
        }

        @Override
        public void run() {
            if ( data == null ) {
                synchronized (list) {
                    if ( data == null ) data = factory.pack(list);
               }
            }
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
                run();
                assert(data != null);
                return data;
            } finally {
                data = null;
                factory = null;
            }
        }
    }
}
