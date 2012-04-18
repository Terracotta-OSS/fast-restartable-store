/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author mscott
 */
public class StackingCommitList implements CommitList {

    private final LogRecord[] regions;
// set at construction    
    private boolean dochecksum = false;
    private long baseLsn;
//  half synchronized
    private volatile boolean syncing = false;
//  these are synchronized     
    private long endLsn;
    private boolean closed = false;
    private boolean written = false;
    private int count = 0;
    
    private final Object guard = new Object();
    private volatile StackingCommitList next;
    
    public StackingCommitList(boolean useChecksum, long startLsn, int maxSize) {
        baseLsn = startLsn;
        endLsn = startLsn-1;
        regions = new LogRecord[maxSize];
        this.dochecksum = useChecksum;
    }

    @Override
    public void setBaseLsn(long lsn) {
        assert(count==0);
        assert(next==null);
        baseLsn = lsn;
        endLsn = baseLsn - 1;
    }
    
    

     @Override
   public boolean append(LogRecord record) {
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length + baseLsn) {
            return false;
        }

        regions[(int) (record.getLsn() - baseLsn)] = record;

        if (!countRecord(record.getLsn())) {
            regions[(int) (record.getLsn() - baseLsn)] = null; //  just to be clean;
            return false;
        }

        return true;
    }

    @Override
    public StackingCommitList next() {
        if (next == null) {
            synchronized (this) {
                if ( !closed ) {
                    closed = true;
                }
                if (next == null) {
                    next = new StackingCommitList(dochecksum, endLsn + 1, regions.length);
                }
            }
        }
        return next;
    }

     @Override
   public long getBaseLsn() {
        return baseLsn;
    }

    @Override
    public long getEndLsn() {
        return endLsn;
    }
    //  TODO:  make more concurrent
    private synchronized boolean countRecord(long lsn) {
        if (closed) {
            if (lsn > endLsn) {
                return false;
            }
        } else if (lsn > endLsn) {
            endLsn = lsn;
        }
        if (count++ == endLsn - baseLsn) {
            this.notify();  // adding one will make count match slots
        }
        
        return true;
    }

    @Override
    public synchronized boolean close(long lsn, boolean sync) {
        if ( lsn <= endLsn ) {
            if ( sync ) syncing = true;
            closed = true;
            this.notify();
        }

        return closed;
    }

    @Override
    public boolean isSyncRequested() {
        return syncing;
    }

    private void waitForWrite() throws InterruptedException {
        synchronized (guard) {
            while (!this.written) {
                guard.wait();
            }
        }
    }

    private void waitForWrite(long millis) throws InterruptedException {
        long span = System.currentTimeMillis();
        synchronized (guard) {
            while (!this.written) {
                if (System.currentTimeMillis() - span > millis) {
                    return;
                }
                guard.wait(millis);
            }
        }
    }

    private boolean isWritten() {
        synchronized (guard) {
            return written;
        }
    }

    @Override
    public void written() {
        synchronized (guard) {
            written = true;
            guard.notifyAll();
        }
    }

    @Override
    public synchronized void waitForContiguous() throws InterruptedException {
        boolean timedout = false;
        while ((!closed && count != regions.length-1) || (closed && count != endLsn - baseLsn + 1)) {
            this.wait(10000);
            if ( timedout ) {
                if ( count > 0 ) {
                    this.close(baseLsn + count - 1,true);
                    timedout = false;
                }
            } else {
                timedout = true;
            }
            
        }
        if (count != endLsn - baseLsn + 1) {
            throw new AssertionError();
        }
    }
    
 //  Future interface

    @Override
    public boolean cancel(boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        this.waitForWrite();
        return null;
    }

    @Override
    public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
        this.waitForWrite(tu.convert(l, TimeUnit.MILLISECONDS));
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return this.isWritten();
    } 
    
    
//  iterator interface
    @Override
    public Iterator<LogRecord> iterator() {
        assert(closed);
        assert(count == endLsn - baseLsn +1);
        
        return new Iterator<LogRecord>() {
            int current = 0;
            @Override
            public boolean hasNext() {
                return ( current < count );
            }

            @Override
            public LogRecord next() {
                return regions[current++];
            }

            @Override
            public void remove() {

            }
        };
    }

}
