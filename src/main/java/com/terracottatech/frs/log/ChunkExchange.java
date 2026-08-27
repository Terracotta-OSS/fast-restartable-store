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
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class ChunkExchange implements Iterable<LogRecord>, Future<Void> {

    private final String forceLogRegionFormat;
    private final BlockingQueue<Future<List<LogRecord>>> queue;
    private final ExecutorService    chunkProcessor;
    private final IOManager io;
    private volatile boolean ioDone = false;
    private volatile int count = 0;
    private final AtomicInteger returned = new AtomicInteger(0);
    private long lastLsn = -1;
    private long lowestLsn = -1;
    private Exception exception;
    private Thread runner;
    private final RecordIterator master;
    private long totalRead;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    ChunkExchange(IOManager io, String forceLogRegionFormat, int maxQueue) {
        this.io = io;
        this.forceLogRegionFormat = forceLogRegionFormat;
        queue = new LinkedBlockingQueue<Future<List<LogRecord>>>(maxQueue);
        chunkProcessor = Executors.newCachedThreadPool(new ThreadFactory() {
            int count = 1;
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("unpack thread - " + count++);
                return t;
            }
        });
        master = new RecordIterator();
    }

    public int returned() {
        return returned.get();
    }

    public int count() {
        return count;
    }

    public synchronized long getLastLsn() throws InterruptedException {
        while (exception == null && lastLsn < 0) {
            this.wait();
        }
        checkReadException();
        return lastLsn;
    }

    public synchronized long getLowestLsn() throws InterruptedException {
        // Lowest LSN does have a possibility of being -1, so just check lastLsn for the
        // loop condition.
        while (exception == null && lastLsn < 0) {
            this.wait();
        }
        checkReadException();
        return lowestLsn;
    }
    
    private synchronized void checkReadException() {
        if ( exception != null ) {
            throw new RuntimeException(exception);
        }
    }

    public synchronized void offerLsns(long lowest, long last) {
        if (lastLsn > 0) {
            return;
        }
        if (last < Constants.FIRST_LSN) {
            last = Constants.GENESIS_LSN;
        }
        lastLsn = last;
        lowestLsn = lowest;
        this.notify();
    }
    
    private synchronized void exceptionThrownInRecovery(Exception exp) {
        exception = exp;
        this.notifyAll();
    }

    void recover() {
        runner = new Thread() {

            @Override
            public void run() {
                readLoop();
            }
        };
        runner.setDaemon(true);
        runner.setName("Recovery Exchange");
        runner.start();
    }

    private long readLoop() {
        long waiting = 0;
        long reading = 0;
        long fill = 0;
        Chunk chunk = null;

        try {
            io.seek(IOManager.Seek.END.getValue());
            chunk = io.read(Direction.REVERSE);
            long last = System.nanoTime();
            boolean first = true;
            while (chunk != null && !master.isDone()) {
                totalRead += chunk.length();
                reading += (System.nanoTime() - last);
                last = System.nanoTime();
                fill += queue.size();
                ChunkProcessing cp = new ChunkProcessing(chunk, forceLogRegionFormat);
                Future<List<LogRecord>> f = chunkProcessor.submit(cp);
                while ( f != null ) {
                  try {
                    queue.put(f);
                    f = null;
                  } catch ( InterruptedException ie ) {
                    if ( master.isDone() ) {
     //  done, clean up
                      for ( LogRecord l : f.get() ) {
                        l.close();
                      }
    //  chunk already processed
                      chunk = null;
                      throw ie;
                    }
                  }
                }
                count += 1;
                waiting += (System.nanoTime() - last);
                last = System.nanoTime();
                chunk = io.read(Direction.REVERSE);
                if (first) {
                    offerLsns(io.getMinimumMarker(), io.getCurrentMarker());
                    first = false;
                }
            }
            if (first) {
                offerLsns(Constants.GENESIS_LSN, Constants.GENESIS_LSN);
            }
       } catch (InterruptedException i) {
            if ( !master.isDone() ) {
 //  unplanned interrupt
                exceptionThrownInRecovery(i);
            }
        } catch (IOException ioe) {
            if ( !master.isDone() ) {
                exceptionThrownInRecovery(ioe);
            }
        } catch ( RuntimeException t ) {
            if ( !master.isDone() ) {
                exceptionThrownInRecovery(t);
            }
        } catch ( Throwable t ) {
          throw new AssertionError(t);
        } finally {
            if ( chunk != null && chunk instanceof Closeable ) {
                try {
                  ((Closeable)chunk).close();
                } catch ( IOException ioe ) {
                  if ( !master.isDone() ) {
                      exceptionThrownInRecovery(ioe);
                  }
                }
            }
            cleanup();
            ioDone = true;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(logread)== waiting: %.3f active: %.3f queue: %d",
                    waiting * 1e-6, reading * 1e-6, (count == 0) ? 0 : fill / count).out().toString());
        }
        return totalRead;
    }

    private void cleanup() {
        try {
            io.seek(IOManager.Seek.BEGINNING.getValue());
            chunkProcessor.shutdown();

            while ( !chunkProcessor.isTerminated() ) {
              try {
                chunkProcessor.awaitTermination(10, TimeUnit.SECONDS);
              } catch ( InterruptedException ie ) {
                LOGGER.debug("cleanup interrupted", ie);
              }            
            }
        } catch ( IOException ioe ) {
            LOGGER.info("unable to shutdown recovery",ioe);
        }
    }

    long getTotalRead() {
        return totalRead;
    }

    private synchronized void waitForDone(long t, TimeUnit tu) throws InterruptedException {
        runner.join(tu.toMillis(t));
        master.waitForIterator();
    }

    @Override
    public boolean cancel(boolean bln) {
        ioDone = true;
        master.setDone();
        return true;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        waitForDone(0, TimeUnit.MILLISECONDS);
        return null;
    }

    @Override
    public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
        waitForDone(l, tu);
        return null;
    }

    @Override
    public boolean isCancelled() {
        return ioDone;
    }
    
    private void drainQueue() {
        Future<List<LogRecord>> result = queue.poll();
        while ( result != null ) {
            try {
              List<LogRecord> list = result.get();
              for ( LogRecord lr : list ) {
                lr.close();
              }
            } catch ( IOException ioe ) {
                LOGGER.warn("possible resource leak",ioe);
            } catch ( ExecutionException ex ) {
                LOGGER.warn("possible resource leak",ex.getCause());
            } catch (InterruptedException ie) {
                LOGGER.warn("possible resource leak",ie);
            }
            result = queue.poll();
        }
    }

    @Override
    public synchronized boolean isDone() {
        return ioDone && master.isDone();
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return master;
    }

    class RecordIterator implements Iterator<LogRecord> {

        long loaded = 0;
        long unloaded = 0;
        long recordCount = 0;
        long recordWait = 0;
        long recordMiss = 0;
        long lsn;
        volatile boolean isDone = false;
        List<LogRecord> list = Collections.<LogRecord>emptyList();

        public RecordIterator() {

        }

        @Override
        public String toString() {
            return "RecordIterator{" + "loaded=" + loaded + ", unloaded=" + unloaded + ", recordCount=" + recordCount + ", recordMiss=" + recordMiss + '}';
        }

        @Override
        public boolean hasNext() {
            if ( isDone ) return false;
            
            while ( list.isEmpty() ) {
                if ( ioDone && queue.isEmpty() ) {
                    break;
                }
                
                try {
                    Future<List<LogRecord>> pre = queue.poll(3, TimeUnit.MILLISECONDS);
                    if ( pre != null ) {
                        long nano = System.nanoTime();
                        list = pre.get();
                        recordWait += (System.nanoTime() - nano);
                        recordCount += 1;
                    } else {
             //  if nothing is left in the queue, an exception may have occured on the read 
             //  thread
                        checkReadException();
                        recordMiss += 1;
                    }
                } catch ( ExecutionException ex ) {
                    throw new RuntimeException(ex.getCause());
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }
            
            if ( list.isEmpty() || list.get(0).getLsn() < lowestLsn) {
                setDone();
                return false;
            } else {
                return true;
            }
        }

        @Override
        public LogRecord next() {
            if ( isDone ) {
                throw new NoSuchElementException("no more records to recover");
            }
            if (list.isEmpty() && !hasNext()) {
                throw new NoSuchElementException();
            }
            LogRecord head = list.remove(0);
            
            if ( head.getLsn() < lowestLsn ) {
                setDone();
                throw new NoSuchElementException("earliest valid record has been already been recovered " + head.getLsn() + " < " + lowestLsn);
            }
            
            lsn = head.getLsn();
            assert (lsn <= lastLsn);

            recordCount += 1;
            return head;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        synchronized void waitForIterator() {
            try {
                while (!isDone) {
                    this.wait();
                }
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }

        boolean isDone() {
            return isDone;
        }

        synchronized void setDone() {
            checkReadException();
            if ( lowestLsn >= 100 && lsn != lowestLsn) {
                throw new RuntimeException("bad recovery lowest lsn: " + lowestLsn + " lsn:" + lsn);
            } else {
                LOGGER.debug("lowest lsn: " + lowestLsn + " lsn:" + lsn);
            }
            isDone = true;
            this.notifyAll();

            for ( LogRecord lr : list ) {
              try {
                lr.close();
              } catch ( IOException ioe ) {
                throw new RuntimeException(ioe);
              }
            }
            
            runner.interrupt();
            try {
              runner.join();
            } catch ( InterruptedException ie ) {
              throw new RuntimeException("recovery interrupted");
            }
            
            drainQueue();
            
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(readIterator)== loaded: %d unloaded: %d count: %d miss: %d avg. wait: %d",
                        loaded, unloaded, recordCount, recordMiss, (recordCount == 0 ) ? 0 : recordWait/recordCount).out().toString());
            }
        }
    }
}
