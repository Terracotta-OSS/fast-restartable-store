/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import java.io.Closeable;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Collections;
import java.util.Formatter;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

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
    private final AtomicLong highestOnDisk = new AtomicLong(Constants.GENESIS_LSN);
    private Signature  checksumStyle;
    private final IOManager io;
    private volatile LogMachineState   state = LogMachineState.IDLE;
    
    private int MAX_QUEUE_SIZE;
    private int RECOVERY_QUEUE_SIZE = 64;
    private String forceLogRegionFormat;

    private ChunkExchange                               exchanger;
    private final BlockingQueue<WritingPackage>         queue = new ArrayBlockingQueue<WritingPackage>(8);
    
    private BufferSource    buffers;

    public StagingLogManager(IOManager io) {
        this(Signature.ADLER32,new AtomicCommitList( Constants.FIRST_LSN, 1024, 200),io, null);
    }
        
    public StagingLogManager(IOManager io, BufferSource src, Configuration config) {
        this(Signature.ADLER32,new AtomicCommitList( Constants.FIRST_LSN, 1024, 200),io, src);
        String checksum = config.getString(FrsProperty.IO_CHECKSUM);
        this.checksumStyle = Signature.valueOf(checksum);
        this.forceLogRegionFormat = config.getString(FrsProperty.FORCE_LOG_REGION_FORMAT);
        this.MAX_QUEUE_SIZE = config.getInt(FrsProperty.IO_COMMIT_QUEUE_SIZE);
        this.RECOVERY_QUEUE_SIZE = config.getInt(FrsProperty.IO_RECOVERY_QUEUE_SIZE);
        String commitList = config.getString(FrsProperty.IO_COMMITLIST);
        if ( commitList.equals("ATOMIC") ) {
            this.currentRegion = new AtomicCommitList(Constants.FIRST_LSN, MAX_QUEUE_SIZE, config.getInt(FrsProperty.IO_WAIT));
        } else if ( commitList.equals("STACKING") ) {
            this.currentRegion = new StackingCommitList(Constants.FIRST_LSN, MAX_QUEUE_SIZE, config.getInt(FrsProperty.IO_WAIT));
        }

    }

    public StagingLogManager(Signature check, CommitList list, IOManager io, BufferSource src) {
        this.currentRegion = list;
        this.io = io;
        currentLsn.set(list.getBaseLsn());
        this.checksumStyle = check;
        this.forceLogRegionFormat = (String) FrsProperty.FORCE_LOG_REGION_FORMAT.defaultValue();
        this.MAX_QUEUE_SIZE = 1024;
        this.buffers =  ( src != null ) ? src : new MaskingBufferSource(new SplittingBufferSource(512,16 * 1024 * 1024));
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
                LOGGER.debug("cleaning was interrupted",in);
     //  reset interrupt
                Thread.currentThread().interrupt();
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
        if ( lowest < Constants.FIRST_LSN ) lowest = Constants.FIRST_LSN;
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
      private final LogRegionFactory  regionFactory = new CopyingPacker(checksumStyle, forceLogRegionFormat, buffers);
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
              if ( wp.isEmpty() ) {
                  continue;
              } else {
                  wp.run();                
              }

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
                      oldRegion.getWriteFuture().get();
                  } catch ( ExecutionException ee ) {
                      //  ignore this, this wait is a performace optimization
                  }
              }
            } catch (InterruptedException ie) {
              oldRegion.exceptionThrown(ie);
              state = state.checkException(ie);
            } catch ( Exception t ) {
              oldRegion.exceptionThrown(t);
              state = state.checkException(t);
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
                
        while ( state.acceptRecords() || currentLsn.get() - 1 != highestOnDisk.get()) {
            long start = System.nanoTime();
            try {
              WritingPackage packer = queue.take();
              long taken = System.nanoTime();
              try {
                waiting += (taken - start);

                if (packer.isEmpty()) {
                  io.sync();
                  continue;
                }

                Chunk c = packer.take();
                if (io.getCurrentMarker() + 1 != packer.baseLsn()) {
                  throw new AssertionError("lsns not sequenced " + io.getCurrentMarker() + 1 + " != " + packer.baseLsn());
                }

                written += io.write(c, packer.endLsn());

                if (c instanceof Closeable) {
                  ((Closeable) c).close();
                }

                if (packer.doSync()) {
                  io.sync();
                }

                highestOnDisk.set(packer.endLsn());
                packer.written();
              } catch (Exception e) {
                packer.list.exceptionThrown(e);
                state = state.checkException(e);
                break;
              } finally {
                writing += (System.nanoTime() - taken);
              }
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
            LOGGER.debug("==PERFORMANCE(memory)==" + buffers.toString());
        }
      }
    }
    
    private Future<Void> recover() {        
        ChunkExchange ex = new ChunkExchange(io, forceLogRegionFormat, RECOVERY_QUEUE_SIZE);
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

        queueEmptyWritingPackageForShutdown();

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
        } catch ( RuntimeException re ) {
            LOGGER.error("error during shutdown",re);
        }
        
        try {
            io.close();
        } catch ( IOException ioe ) {
//  log io error and close            
            LOGGER.error("error closing io",ioe);
        }
        state = state.idle();
    }

  private void queueEmptyWritingPackageForShutdown() {
    // this tries to put an empty WritingPackage on the queue
    // so the daemon wakes up. This speeds shutdown,
    // as it will wake the daemon thread.
    queue.offer(new WritingPackage(new CommitList() {
    @Override
      public Future<Void> getWriteFuture() {
        return CompletableFuture.completedFuture(null);
      }
        
      @SuppressWarnings("unchecked")
      @Override
      public Iterator<LogRecord> iterator() {
        return Collections.EMPTY_LIST.iterator();
      }

      @Override
      public boolean append(LogRecord record, boolean sync) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean close(long lsn) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void waitForContiguous() throws InterruptedException {
        throw new UnsupportedOperationException();
      }

      @Override
      public CommitList next() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isSyncRequested() {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean isEmpty() {
        return true;
      }

      @Override
      public long getEndLsn() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long getBaseLsn() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void written() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void exceptionThrown(Exception exp) {
        throw new UnsupportedOperationException();
      }

      @Override
      public CommitList create(long baseLsn) {
        throw new UnsupportedOperationException();
      }
    }, null));
  }

  private CommitList _append(LogRecord record, boolean sync) {
        if ( !state.acceptRecords() ) {
          throw new LogWriteError();
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
                    futureWait(mine);
                    waitspin += (Math.round((float)(Math.random() * 512f)));
                }
                mine = mine.next();
            }
        }
        return mine;
    }
        
    private void futureWait(CommitList mine) {
      try {                      
          mine.getWriteFuture().get();
      } catch ( InterruptedException ie ) {

      } catch ( ExecutionException ee ) {

      }
    }

    @Override
    public Future<Void> append(LogRecord record) {
// this is not a guaranteed write.  consider returning a future that throws an exception
        return _append(record,false).getWriteFuture();
      }

    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        return _append(record,true).getWriteFuture();
    }

    @Override
    public Snapshot snapshot() throws ExecutionException {
        SnapshotRecord snapshot = new SnapshotRecord();
        boolean interrupted = false;
        Future<Void> write = this.append(snapshot);

        while (true) {
          try {
            write.get();
            break;
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }
        
        if (interrupted) {
          Thread.currentThread().interrupt();
          return null;
        }
        
        return snapshot;
    }

  @Override
  public Future<Snapshot> snapshotAsync() {
    SnapshotRecord snapshot = new SnapshotRecord();
    Future<Void> write = this.append(snapshot);
    return new SnapshotFuture(snapshot, write);
  }

  @Override
    public IOStatistics getIOStatistics() {
      try {
        return io.getStatistics();
      } catch ( IOException ioe ) {
        LOGGER.error("error collecting io statistics",ioe);
        return new IOStatistics() {

          @Override
          public long getTotalAvailable() {
            return 0;
          }

          @Override
          public long getTotalUsed() {
            return 0;
          }

          @Override
          public long getTotalWritten() {
            return 0;
          }

          @Override
          public long getTotalRead() {
            return 0;
          }

          @Override
          public long getLiveSize() {
            return 0;
          }

          @Override
          public long getExpiredSize() {
            return 0;
          }
        };
      }
    }

    static class WritingPackage implements Runnable {
        /**
         * list of writes to make
         */
        private final CommitList                list;
        /**
         * used to pack the byte buffer
         */
        private volatile LogRegionFactory      factory;
        /**
         * bytes to write to disk
         */
        private volatile Chunk                  data;
        
        WritingPackage(CommitList list, LogRegionFactory factory) {
            this.list= list;
            this.factory = factory;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            if ( data == null ) {
                synchronized (list) {
                   if ( data == null ) {
                      data = factory.pack(list);
                   }
                }
            }
        }
        
        boolean isEmpty() {
            return list.isEmpty();
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

  private static class SnapshotFuture implements Future<Snapshot> {
    private final Snapshot snapshot;
    private final Future<Void> write;

    private SnapshotFuture(Snapshot snapshot, Future<Void> write) {
      this.snapshot = snapshot;
      this.write = write;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return write.isDone();
    }

    @Override
    public Snapshot get() throws InterruptedException, ExecutionException {
      write.get();
      return snapshot;
    }

    @Override
    public Snapshot get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      write.get(timeout, unit);
      return write.isDone() ? snapshot : null;
    }
  }
}