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
    private static final LogRecord DUMMY_RECORD = new LogRecordImpl(0, null, null);

    private final Object guard = new Object();
    private final AtomicReferenceArray<LogRecord> regions;

    private final AtomicLong endLsn;
    private final CountDownLatch goLatch;

    private final long baseLsn;
    private volatile long lowestLsn;
    private final AtomicLong syncRequest = new AtomicLong();
    private boolean written = false;
    private volatile CommitList next;

    public AtomicCommitList( long startLsn, int maxSize) {
        baseLsn = startLsn;
        endLsn = new AtomicLong();
        regions = new AtomicReferenceArray<LogRecord>(maxSize);
//        this.doChecksum = useChecksum;
        goLatch = new CountDownLatch(maxSize);
    }
    
    @Override
    public boolean append(LogRecord record, boolean sync) {
        if ( record == null ) return true;
        
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length() + baseLsn) {
            return false;
        }
        
        long end = endLsn.get();
        
        if ( end > 0 && end < record.getLsn() ) return false;
        
        if ( sync ) {
   //  if a sync is requested, go ahead and set the flag whether the 
   //  record actually gets dropped here we need to ensure the sync request moves
   //  with the highest lsn that reqested it.  a couple extra syncs are ok.
            setSyncRequest(record.getLsn());
        }
        
        if ( regions.compareAndSet((int) (record.getLsn() - baseLsn), null, record) ) {
            if ( record.getLowestLsn() > lowestLsn ) lowestLsn = record.getLowestLsn();
            goLatch.countDown();
        } else {
            return false;
        }


        return true;
    }

    @Override
    public long getLowestLsn() {
        return lowestLsn;
    }
    
    
    private void setSyncRequest(long newRequest) {
        long csync = syncRequest.get();
        while (  csync < newRequest ) {
            if ( !syncRequest.compareAndSet(csync, newRequest) ) {
                csync = syncRequest.get();
            }
        }
    }

    @Override
    public CommitList next() {
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
                if ( next == null ) next = create(nextLsn);
            }
        }
        return next;
    }
    
    public CommitList create(long nextLsn) {
        return new AtomicCommitList( nextLsn, regions.length());
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
    public boolean isEmpty() {
        return baseLsn > endLsn.get();
    }

    @Override
    public boolean close(long end) {
        boolean closer;
                
        if ( end >= baseLsn + regions.length() ) {
            return false;
        }
        
        closer = endLsn.compareAndSet(0, end);
              
        if ( closer ) {
            transferOutExtras();
        } else {
     // if not set here make sure this end is within the range, if not return false;
            return ( end <= endLsn.get() );
        }
                        
        return true;
    }


    private void transferOutExtras() {
        CommitList cnext = next(); 
        int offset = (int)(endLsn.get()-baseLsn+1);
        for (int x=offset;x<regions.length();x++) {
            LogRecord record = regions.getAndSet(x, DUMMY_RECORD);
            if ( record == null ) {
                goLatch.countDown();
            } else {
                CommitList nnext = cnext;
        //  TODO: is this check necessary?  
                while ( !nnext.append(record, syncRequest.get() >= record.getLsn()) ) {
                    nnext = nnext.next();
                }
            }
        }
    }

    @Override
    public boolean isSyncRequested() {
        return syncRequest.get() > 0;
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
        for(int x = 0 ; x < endLsn.get() - baseLsn + 1; x++) {
            if (regions.get(x) == null || regions.get(x).getLsn() != (baseLsn + x)) {
                return false;
            }
        }
        if (endLsn.get() - baseLsn + 1 == 0) return true;  //  no records here
        return true;
    }

    @Override
    public void waitForContiguous() throws InterruptedException {
        if ( goLatch.getCount() != regions.length() ) {
            checkForClosed();
        } else {
        }
        
        while ( !goLatch.await(20, TimeUnit.MILLISECONDS) ) {
            if ( goLatch.getCount() != regions.length() ) {
                checkForClosed();
            }
        }
        if ( endLsn.compareAndSet(0, baseLsn + regions.length() -1) ) {
 // filled all the slots with no close.  set endLsn now
            assert(goLatch.getCount() == 0);
        }
        assert(baseLsn == 0 || checkValues());
    }
    
 //  Future interface
    @Override
    public boolean cancel(boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    private void checkForClosed() {
        if ( endLsn.get() == 0 ) {
            int size = regions.length() - (int)goLatch.getCount();
            LogRecord record = null;
            while ( record == null && size-- > 0) {
                record = regions.get(size);
            }
            if ( size < 0 ) {
//                this.close(baseLsn-1, false);
            } else {
                this.close(record.getLsn());
            }
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        checkForClosed();
        this.waitForWrite();
        return null;
    }

    @Override
    public Void get(long time, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
        checkForClosed();
        this.waitForWrite(tu.convert(time, TimeUnit.MILLISECONDS));
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return (goLatch.getCount() == regions.length() || this.isWritten());
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
