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
    private long lowestLsn = -1;
    private Exception exception;


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
    
    public synchronized void offerLsns(long lowest, long last) {
        if ( lastLsn > 0 ) return;
        lastLsn = last;
        lowestLsn = lowest;
        this.notify();
    }

    long recover() {
        long waiting = 0;
        long reading = 0;
        long totalRead = 0;
        long fill = 0;
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
                    fill += queue.size();
                    queue.put(chunk);
                    waiting += (System.nanoTime() - last);
                    last = System.nanoTime();
                }
            } while (chunk != null && !done);
            if ( lastLsn < 0 ) {
                offerLsns(99,99);
            }
        } catch (InterruptedException i) {
            exception = i;
        } catch (IOException ioe) {
            exception = ioe;
        } finally {
            done = true;
        }
        System.out.format("read -- waiting: %d active: %d ave queue: %d\n",waiting,reading,(count==0)? 0 : fill/count);
        return totalRead;
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return new Iterator<LogRecord>() {
            
            boolean first = true;
            Deque<LogRecord> list = new ArrayDeque<LogRecord>(Collections.<LogRecord>emptyList());

            @Override
            public boolean hasNext() {
                if ( exception != null ) throw new RuntimeException(exception);
                if ( list.isEmpty()) {
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
                                    if ( first ) {
                                        offerLsns(record.getLowestLsn(),record.getLsn());
                                        first = false;
                                    } 
                                }
                            }
                        }
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    } catch ( RuntimeException ioe ) {
                        done = true;
                        queue.clear();
                        throw new RuntimeException(ioe);
                    }
                }
                
                if ( !list.isEmpty() ) {
  //  check to see if iterator is past the lowestLsn.  If so, no need to return any more records.       
                    if ( list.peek().getLsn() < lowestLsn ) {
                        done = true;
                        // TODO: This is a total hack to work around the race between finishing
                        // the iteration and the reader thread blocking on the queue.
                        queue.clear();
                        return false;
                    } else {
                        return true;
                    }
                } else {
                    return false;
                }
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
