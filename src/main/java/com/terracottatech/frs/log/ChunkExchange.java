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

/**
 *
 * @author mscott
 */
public class ChunkExchange implements Runnable, Iterable<LogRecord> {

    private final ArrayBlockingQueue<LogRecord> queue = new ArrayBlockingQueue<LogRecord>(1000);
    private final IOManager io;
    private final LogRegionFactory packer = new LogRegionPacker(new MasterLogRecordFactory(), Signature.ADLER32);
    private volatile boolean done = false;

    public ChunkExchange(IOManager io) {
        this.io = io;
    }

    @Override
    public void run() {
        try {
            io.seek(Seek.END.getValue());
            Iterable<Chunk> chunks;
            do {
                chunks = io.read(Direction.REVERSE);
                if (chunks != null) {
                    for (Chunk c : chunks) {
                        List<LogRecord> records = packer.unpack(c);
                        Collections.reverse(records);
                        for (LogRecord record : records) {
                            queue.offer(record);
                        }
                    }
                }
            } while (chunks != null);
        } catch (IOException ioe) {
            throw new AssertionError();
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
                    if ( done && queue.isEmpty() ) return false;
                    while ( queued == null) {
                        queued = queue.poll(1, TimeUnit.SECONDS);
                        if ( done ) break;
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
