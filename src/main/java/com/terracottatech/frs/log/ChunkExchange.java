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

    private final ArrayBlockingQueue<Chunk> queue;
    private final IOManager io;
    private volatile boolean ioDone = false;
    private volatile int count = 0;
    private final AtomicInteger returned = new AtomicInteger(0);
    private long lastLsn = -1;
    private long lowestLsn = -1;
    private Exception exception;
    Thread runner;
    private RecordIterator master = new RecordIterator();
    private long totalRead;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

    ChunkExchange(IOManager io, Signature style, int maxQueue) {
        this.io = io;
        queue = new ArrayBlockingQueue<Chunk>(maxQueue);
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
        if ( last < 100 ) last = 99;
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
        long time = 0;
        try {
            io.seek(IOManager.Seek.END.getValue());
            Chunk chunk = io.read(Direction.REVERSE);
            long last = System.nanoTime();
            boolean first = true;
            long tick = System.currentTimeMillis();
            while (chunk != null && !master.isDone() ) {
                totalRead += chunk.length();
                reading += (System.nanoTime() - last);
                last = System.nanoTime();
                fill += queue.size();
                queue.put(chunk);
                count+=1;
                waiting += (System.nanoTime() - last);
                last = System.nanoTime();
                chunk = io.read(Direction.REVERSE);
                if ( first ) {
                    offerLsns(io.getMinimumMarker(),io.getMaximumMarker());
                    first = false;
                } else {
                    if ( System.currentTimeMillis() - tick > 15 * 1000) {
                        time += tick;
                        tick += System.currentTimeMillis();
//                        if ( LOGGER.isDebugEnabled() ) {
//                            LOGGER.debug(new Formatter(new StringBuilder()).format("Recovery: read rate %.2f", totalRead / (1024f * 1024f * time * 1e-3)).out().toString());
//                        }
                    }
                }
            }
            if ( first ) {
                offerLsns(99, 99);
            }
            cleanup();
        } catch (InterruptedException i) {
            exception = i;
        } catch (IOException ioe) {
            exception = ioe;
            ioe.printStackTrace();
        } finally {
            ioDone = true;
        }
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug(new Formatter(new StringBuilder()).format("==PERFORMANCE(logread)== waiting: %.3f active: %.3f queue: %d", 
                    waiting*1e-6, reading*1e-6, (count == 0) ? 0 : fill / count).out().toString());
//            LOGGER.debug(new Formatter(new StringBuilder()).format("Recovery: read rate %.2f", totalRead / (1024f * 1024f * time * 1e-3)).out().toString());
       }
        return totalRead;
    }
    
    private void cleanup() throws IOException {
        io.seek(IOManager.Seek.BEGINNING.getValue());
        if ( master.isDone() ) {
            queue.clear();
        }
    }
    
    long getTotalRead() {
        return totalRead;
    }

    private synchronized void waitForDone(long t, TimeUnit tu) throws InterruptedException {
        runner.join();
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

        long lsn;
        volatile boolean isDone = false;
        boolean first = true;
        Deque<LogRecord> list = new ArrayDeque<LogRecord>(Collections.<LogRecord>emptyList());

        @Override
        public boolean hasNext() {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            if (list.isEmpty()) {
                try {
                    Chunk queued = null;
                    while (queued == null && list.isEmpty()) {
                        if (ioDone && queue.isEmpty()) {
                            break;
                        }
                        queued = queue.poll(10, TimeUnit.SECONDS);
                        if (queued != null) {
                            returned.incrementAndGet();
                            try {
                                List<LogRecord> records = LogRegionPacker.unpack(Signature.ADLER32, queued);
                                Collections.reverse(records);
                                list = new ArrayDeque<LogRecord>(records);
                            } catch ( ChecksumException ce ) {
                                throw new RuntimeException(ce);
                            }
                        }
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                } catch (RuntimeException ioe) {
                    setDone();
                    throw new RuntimeException(ioe);
                }
            }

            if (!list.isEmpty()) {
                //  check to see if iterator is past the lowestLsn.  If so, no need to return any more records.       
                if (list.peek().getLsn() < lowestLsn) {
                    setDone();
                    // TODO: This is a total hack to work around the race between finishing
                    // the iteration and the reader thread blocking on the queue.
                    return false;
                } else {
                    return true;
                }
            } else {
                setDone();
                return false;
            }
        }

        @Override
        public LogRecord next() {
            if (exception != null) {
                throw new RuntimeException(exception);
            }
            hasNext();
            if (list.isEmpty()) {
                throw new NoSuchElementException();
            }
            try {
                lsn = list.peek().getLsn();
                assert(lsn <= lastLsn);
                return list.removeFirst();
            } finally {

            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    
        synchronized void waitForIterator() {
            while ( !isDone ) {
                try {
                    this.wait();
                } catch ( InterruptedException ie ) {
                    throw new RuntimeException(ie);
                }
            }
        }

        boolean isDone() {
            return isDone;
        } 

        synchronized void setDone() {
            assert (lsn <= lowestLsn );
            isDone = true;
            this.notifyAll();
            queue.clear();
            list = new ArrayDeque<LogRecord>(Collections.<LogRecord>emptyList());
        }
    }
}
 
