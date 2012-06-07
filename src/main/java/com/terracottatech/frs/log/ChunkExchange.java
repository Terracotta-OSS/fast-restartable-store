/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
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

    private final LinkedBlockingDeque<Future<List<LogRecord>>> queue;
    private final ExecutorService    chunkProcessor = Executors.newCachedThreadPool();
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

    ChunkExchange(IOManager io, int maxQueue) {
        this.io = io;
        queue = new LinkedBlockingDeque<Future<List<LogRecord>>>(maxQueue);
        master = new RecordIterator();
    }

    public int returned() {
        return returned.get();
    }

    public int count() {
        return count;
    }

    public synchronized long getLastLsn() throws InterruptedException {
        while (lastLsn < 0) {
            this.wait();
        }
        return lastLsn;
    }

    public synchronized long getLowestLsn() throws InterruptedException {
        // Lowest LSN does have a possibility of being -1, so just check lastLsn for the
        // loop condition.
        while (lastLsn < 0) {
            this.wait();
        }
        return lowestLsn;
    }

    public synchronized void offerLsns(long lowest, long last) {
        if (lastLsn > 0) {
            return;
        }
        if (last < 100) {
            last = 99;
        }
        lastLsn = last;
        lowestLsn = lowest;
        this.notify();
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

        try {
            io.seek(IOManager.Seek.END.getValue());
            Chunk chunk = io.read(Direction.REVERSE);
            long last = System.nanoTime();
            boolean first = true;
            long tick = System.currentTimeMillis();
            while (chunk != null && !master.isDone()) {
                totalRead += chunk.length();
                reading += (System.nanoTime() - last);
                last = System.nanoTime();
                fill += queue.size();
                ChunkProcessing cp = new ChunkProcessing(chunk);
                queue.put(chunkProcessor.submit(cp));
                count += 1;
                waiting += (System.nanoTime() - last);
                last = System.nanoTime();
                chunk = io.read(Direction.REVERSE);
                if (first) {
                    offerLsns(io.getMinimumMarker(), io.getMaximumMarker());
                    first = false;
                } else {
                    if (System.currentTimeMillis() - tick > 15 * 1000) {
                        tick += System.currentTimeMillis();
                    }
                }
            }
            if (first) {
                offerLsns(99, 99);
            }
            cleanup();
        } catch (InterruptedException i) {
            if ( !master.isDone() ) {
 //  unplanned interrupt
                exception = i;
            }
            exception = i;
        } catch (IOException ioe) {
            exception = ioe;
        } finally {
            ioDone = true;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(logread)== waiting: %.3f active: %.3f queue: %d",
                    waiting * 1e-6, reading * 1e-6, (count == 0) ? 0 : fill / count).out().toString());
        }
        return totalRead;
    }

    private void cleanup() throws IOException {
        io.seek(IOManager.Seek.BEGINNING.getValue());
        if (master.isDone()) {
            queue.clear();
        }
        chunkProcessor.shutdown();
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
        queue.clear();
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
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            if ( isDone ) return false;
            
            while ( list.isEmpty() ) {
                if ( ioDone && queue.isEmpty() ) {
                    setDone();
                    return false;
                }
                
                try {
                    Future<List<LogRecord>> pre = queue.poll(3, TimeUnit.MILLISECONDS);
                    
                    if ( pre != null ) list = pre.get();
                } catch ( ExecutionException ex ) {
                    throw new RuntimeException(ex.getCause());
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }

            if (list.isEmpty() || list.get(0).getLsn() < lowestLsn) {
                setDone();
                return false;
            } else {
                return true;
            }
        }

        @Override
        public LogRecord next() {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            if ( isDone ) {
                throw new NoSuchElementException();
            }
            if (list.isEmpty() && !hasNext()) {
                throw new NoSuchElementException();
            }
            LogRecord head = list.remove(0);
            lsn = head.getLsn();
            assert (lsn <= lastLsn);
            try {
                recordCount += 1;
                return head;
            } finally {
                head = null;
            }
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
            assert (lowestLsn < 100 || lsn <= lowestLsn);
            isDone = true;
            this.notifyAll();
            queue.clear();
            list.clear();
            runner.interrupt();
            chunkProcessor.shutdownNow();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(readIterator)== loaded: %d unloaded: %d count: %d miss: %d",
                        loaded, unloaded, recordCount, recordMiss).out().toString());
            }
        }
    }
}
