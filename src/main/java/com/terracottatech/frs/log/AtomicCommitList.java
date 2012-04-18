/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 * @author mscott
 */
public class AtomicCommitList implements CommitList, Future<Void> {

    private final AtomicReferenceArray<LogRecord> regions;
// set at construction    
    private final boolean dochecksum;
    private long baseLsn;

    private volatile boolean syncing = false;

    private final AtomicLong endLsn;
    private final CountDownLatch golatch;
    
    private boolean written = false;
    
    private final Object guard = new Object();
    private volatile AtomicCommitList next;
    
    private static final LogRecord DUMMY_RECORD = new LogRecordImpl(0, null, null);
    
    public AtomicCommitList(boolean useChecksum, long startLsn, int maxSize) {
        baseLsn = startLsn;
        endLsn = new AtomicLong();
        regions = new AtomicReferenceArray<LogRecord>(maxSize);
        this.dochecksum = useChecksum;
        golatch = new CountDownLatch(maxSize);
    }
    
    public void setBaseLsn(long lsn) {
        assert(next == null);
        assert(golatch.getCount() == regions.length());
        baseLsn = lsn;
    }
    
    @Override
    public boolean append(LogRecord record) {
        if ( record == null ) return true;
        
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length() + baseLsn) {
            return false;
        }
        
        long end = endLsn.get();
        
        if ( end > 0 && end < record.getLsn() ) return false;
        
        if ( regions.compareAndSet((int) (record.getLsn() - baseLsn), null, record) ) {
            golatch.countDown();
        } else {
            return false;
        }


        return true;
    }

    /**
     *  
     *
     * @return
     */
    @Override
    public AtomicCommitList next() {
        if ( next == null ) {
            long nextLsn = endLsn.get() + 1;
            if ( nextLsn == 1 ) {
                if ( endLsn.compareAndSet(0, baseLsn + regions.length()-1) ) { 
                    nextLsn = baseLsn + regions.length();
                } else {
                    nextLsn = endLsn.get() + 1;
                }
            }
            synchronized (guard) {
                if ( next == null ) next = new AtomicCommitList(dochecksum, nextLsn, regions.length());
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
        return endLsn.get();
    }

    @Override
    public boolean close(long end, boolean sync) {
        if ( sync ) syncing = true;
        boolean closer;
        
        if ( end >= baseLsn + regions.length() ) {
            return false;
        }
        
        closer = endLsn.compareAndSet(0,end);
              
        if ( closer ) {
            transferOutExtras();
        } else {
     // if not set here make sure this end is within the range, if not return false;
            return ( end <= endLsn.get() );
        }
                
        return true;
    }


    private void transferOutExtras() {
        AtomicCommitList cnext = next(); 
        int offset = (int)(endLsn.get()-baseLsn+1);
        for (int x=offset;x<regions.length();x++) {
            LogRecord record = regions.getAndSet(x, DUMMY_RECORD);
            if ( record == null ) golatch.countDown();
            else cnext.append(record);
        }
    }
    
    
    @Override
    public boolean isSyncing() {
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
    
    private boolean checkValues() {
        for(int x=0;x<(int)(endLsn.get()-baseLsn+1);x++) {
            if (regions.get(x) == null) {
                return false;
            }
        }
        if (endLsn.get()-baseLsn+1 == 0) return true;  //  no records here
        if (baseLsn != regions.get(0).getLsn() ) return false;
        if (endLsn.get() != regions.get((int)(endLsn.get()-baseLsn)).getLsn()) return false;
        return true;
    }
    
    

    @Override
    public void waitForContiguous() throws InterruptedException {
        while ( !golatch.await(10, TimeUnit.SECONDS) ) {
            if ( golatch.getCount() != regions.length() ) {
                this.close(baseLsn + regions.length() + golatch.getCount() - 1,true);
            }
        }
        if ( endLsn.compareAndSet(0, baseLsn + regions.length() -1) ) {
 // filled all the slots with no close.  set endLsn now
            assert(golatch.getCount() == 0);
        }
        assert(baseLsn == 0 || checkValues());
       
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
        return (golatch.getCount() == regions.length() || this.isWritten());
    }   
    
//  iterator interface
    @Override
    public Iterator<LogRecord> iterator() {
        assert(endLsn.get() > 0);
        
        final int total = (int)(endLsn.get() - baseLsn + 1);
        
        return new Iterator<LogRecord>() {
            int current = 0;
            @Override
            public boolean hasNext() {
                return ( current < total );
            }

            @Override
            public LogRecord next() {
                return regions.get(current++);
            }

            @Override
            public void remove() {

            }
        };
    }

}
