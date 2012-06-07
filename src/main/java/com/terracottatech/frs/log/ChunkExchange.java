/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.MappedChunk;
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

    private final ArrayBlockingQueue<Chunk> queue;
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
        queue = new ArrayBlockingQueue<Chunk>(maxQueue);
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
                queue.put(chunk);
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
        if (!master.isAlive() && !master.isDone()) {
            master.start();
        }
        return master;
    }

    class RecordIterator extends Thread implements Iterator<LogRecord> {

        long loaded = 0;
        long unloaded = 0;
        long recordCount = 0;
        long recordMiss = 0;
        long lsn;
        volatile boolean isDone = false;
        volatile boolean complete = false;
        BlockingQueue<LogRecord> list;
        volatile LogRecord head = null;

        public RecordIterator() {
            list = new ArrayBlockingQueue<LogRecord>(8192);
            this.setDaemon(true);
            this.setName("Recovery record unpacker");
        }

        @Override
        public void run() {
            try {
                while (!ioDone || !queue.isEmpty()) {
                    Chunk queued = null;
                    try {
                        if ( isDone ) break;
                        queued = queue.poll(10, TimeUnit.SECONDS);
                        if (queued != null) {
                            returned.incrementAndGet();
                            try {
                                List<LogRecord> records = LogRegionPacker.unpack(Signature.ADLER32, queued);
                                Collections.reverse(records);
                                for (LogRecord r : records) {
                                    if ( !isDone ) list.put(r);
                                }
                            } catch (FormatException ce) {
                                throw new RuntimeException(ce);
                            }
                        } else {
                            LOGGER.debug("!!!!!!!!!!!!recovery queue timeout!!!!!!!!!!!!!!!!");
                        }
                    } catch (InterruptedException ie) {
                        if ( !isDone && queued != null ) {
            //  unplanned interrupt, rethrow
                            throw new RuntimeException(ie);
                        }
                    }
                }
            } finally {
                complete = true;
            }
        }

        @Override
        public String toString() {
            return "RecordIterator{" + "loaded=" + loaded + ", unloaded=" + unloaded + ", recordCount=" + recordCount + ", recordMiss=" + recordMiss + '}';
        }

        @Override
        public boolean hasNext() {
            while (head == null) {
                try {
                    if ( complete && list.isEmpty() ) {
                        setDone();
                        return false;
                    } 
                
                    head = list.poll(3, TimeUnit.MILLISECONDS);
                    
                    if (head != null) {
                        recordCount += 1;
                    } else {
                        recordMiss += 1;
                    }
                        
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }

            if (head.getLsn() < lowestLsn) {
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
            if (head == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            lsn = head.getLsn();
            assert (lsn <= lastLsn);
            try {
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
                this.join();
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
            this.interrupt();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(readIterator)== loaded: %d unloaded: %d count: %d miss: %d",
                        loaded, unloaded, recordCount, recordMiss).out().toString());
            }
        }
    }
}
