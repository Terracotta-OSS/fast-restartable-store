/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.Seek;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author mscott
 */
public class ChunkExchange implements Runnable, Iterable<LogRecord> {

    private final ArrayBlockingQueue<LogRecord> queue = new ArrayBlockingQueue<LogRecord>(1000);
    private final IOManager io;
    private final LogRegionFactory packer;
    private volatile boolean done = false;
    private volatile int  count = 0;
    private final AtomicInteger  returned = new AtomicInteger(0);
    private long lastLsn = -1;

    public ChunkExchange(IOManager io, Signature style) {
        this.io = io;
        packer = new LogRegionPacker(style);
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

    @Override
    public void run() {
        try {
            io.seek(Seek.END.getValue());
            Chunk chunk;
            do {
                chunk = io.read(Direction.REVERSE);
                if (chunk != null) {
                    List<LogRecord> records = packer.unpack(chunk);
                    Collections.reverse(records);
                    for (LogRecord record : records) {
                        if ( lastLsn < 0 ) {
                            offerLastLsn(record.getLsn());
                        }
                        queue.put(record);
                        count++;
                    }
                }
            } while (chunk != null);
            if ( lastLsn < 0 ) {
                offerLastLsn(99);
            }
        } catch (InterruptedException ioe) {
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        } finally {
            done = true;
        }
    }

    @Override
    public Iterator<LogRecord> iterator() {
        return new Iterator<LogRecord>() {
            LogRecord queued;

            @Override
            public boolean hasNext() {
                try {
                    if (  queued == null && done && count == returned.get()  ) return false;
                    while ( queued == null) {
                        queued = queue.poll(10, TimeUnit.SECONDS);
                        if ( done && count == returned.get() ) break;
                    }
                } catch (InterruptedException ie) {
                }

                return queued != null;
            }

            @Override
            public LogRecord next() {
                hasNext();
                if (queued == null) {
                    throw new NoSuchElementException();
                }
                try {
                    returned.incrementAndGet();
                    return queued;
                } finally {
                    queued = null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
    }
}
