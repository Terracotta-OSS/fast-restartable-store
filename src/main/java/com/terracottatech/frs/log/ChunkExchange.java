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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author mscott
 */
public class ChunkExchange implements Iterable<LogRecord> {

    private final ArrayBlockingQueue<Chunk> queue;
    private final IOManager io;
    private volatile boolean done = false;
    private volatile int  count = 0;
    private final AtomicInteger  returned = new AtomicInteger(0);
    private long lastLsn = -1;
    private Exception exception;
    
    private long waiting;
    private long reading;

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
        while ( lastLsn < 0 ) {
            this.wait();
        }
        return lastLsn;
    }
    
    public synchronized void offerLastLsn(long lsn) {
        if ( lastLsn > 0 ) return;
        lastLsn = lsn;
        this.notify();
    }

    long recover() {
        long totalRead = 0;
        try {
            io.seek(IOManager.Seek.END.getValue());
            Chunk chunk;
            long last = System.nanoTime();
            do {
                chunk = io.read(Direction.REVERSE);
                if (chunk != null) {
                    count++;
                    totalRead += chunk.length();
                    reading += (System.nanoTime() - last);
                    last = System.nanoTime();
                    queue.put(chunk);
                    waiting += (System.nanoTime() - last);
                    last = System.nanoTime();
                }
            } while (chunk != null);
            if ( lastLsn < 0 ) {
                offerLastLsn(99);
            }
        } catch (InterruptedException i) {
            exception = i;
        } catch (IOException ioe) {
            exception = ioe;
        } finally {
            done = true;
        }
        System.out.format("read -- waiting: %d active: %d\n",waiting,reading);
        return totalRead;
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return new Iterator<LogRecord>() {
            Deque<LogRecord> list = new ArrayDeque<LogRecord>(Collections.<LogRecord>emptyList());

            @Override
            public boolean hasNext() {
                if ( exception != null ) throw new RuntimeException(exception);
                if ( !list.isEmpty() ) return true;
                try {
                    Chunk queued = null;
                    while ( queued == null && list.isEmpty() ) {
                        if ( done && count == returned.get() ) break;
                        queued = queue.poll(10, TimeUnit.SECONDS);
                        if ( queued != null ) {
                            returned.incrementAndGet();
                            List<LogRecord> records = LogRegionPacker.unpack(Signature.ADLER32, queued);
                            Collections.reverse(records);
                            list = new ArrayDeque<LogRecord>(records);
                            for (LogRecord record : records) {
                                if ( lastLsn < 0 ) {
                                    offerLastLsn(record.getLsn());
                                }
                            }
                        }
                    }
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                } catch ( IOException ioe ) {
                    throw new RuntimeException(ioe);
                }

                return !list.isEmpty();
            }

            @Override
            public LogRecord next() {
                if ( exception != null ) throw new RuntimeException(exception);
                hasNext();
                if (list.isEmpty()) {
                    throw new NoSuchElementException();
                }
                try {
                    return list.removeFirst();
                } finally {

                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
