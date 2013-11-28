/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 * @author mscott
 */
public class AtomicCommitList implements CommitList {
    private static final LogRecord DUMMY_RECORD = new LogRecordImpl(null, null);

    private final Object guard = new Object();
    private final AtomicReferenceArray<LogRecord> regions;

    private final AtomicLong endLsn;
    private final CountDownLatch goLatch;

    private final long baseLsn;
    private final AtomicLong syncRequest = new AtomicLong();
    private boolean written = false;
    private Exception error;
    private volatile CommitList next;
    private final int      wait;
    private volatile boolean        atHead = false;

    public AtomicCommitList(long startLsn, int maxSize,int waitTime) {
        baseLsn = startLsn;
        endLsn = new AtomicLong();
        regions = new AtomicReferenceArray<LogRecord>(maxSize);
        goLatch = new CountDownLatch(maxSize);
        wait = waitTime;
    }
    
    @Override
    public boolean append(LogRecord record, boolean sync) {
        if ( record == null ) return true;
        
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length() + baseLsn) {
            return false;
        }
        
        boolean close = false;
        long end = endLsn.get();
        
        if ( end > 0 && end < record.getLsn() ) return false;
        
        if ( sync ) {
   //  if a sync is requested, go ahead and set the flag whether the 
   //  record actually gets dropped here we need to ensure the sync request moves
   //  with the highest lsn that reqested it.  a couple extra syncs are ok.
            close = setSyncRequest(record.getLsn());
        }
        
        if ( regions.compareAndSet((int) (record.getLsn() - baseLsn), null, record) ) {
            goLatch.countDown();
            if ( atHead && (close || record instanceof SnapshotRequest )) {
              if (syncRequest.get() == record.getLsn() || record instanceof SnapshotRequest) {
                checkForClosed();
              }
            }
        } else {
            return false;
        }


        return true;
    }
    
    private boolean setSyncRequest(long newRequest) {
        long csync = syncRequest.get();
        while (  csync < newRequest ) {
            if ( !syncRequest.compareAndSet(csync, newRequest) ) {
                csync = syncRequest.get();
            } else {
              return true;
            }
        }
        return false;
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
    
    @Override
    public CommitList create(long nextLsn) {
        return new AtomicCommitList( nextLsn, regions.length(), wait);
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
                while ( !cnext.append(record, syncRequest.get() == record.getLsn()) ) {
                    cnext = cnext.next();
                }
            }
        }
    }

    @Override
    public boolean isSyncRequested() {
        return syncRequest.get() > 0;
    }

    private void waitForWrite() throws InterruptedException,ExecutionException {
        waitForWrite(0);
    }

    private void waitForWrite(long millis) throws InterruptedException,ExecutionException {
        long span = System.currentTimeMillis();
        synchronized (guard) {
            while (!this.written) {
                if ( error != null ) throw new ExecutionException(error);
                if (millis != 0 && System.currentTimeMillis() - span > millis) {
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
    

    private boolean isError() {
        synchronized (guard) {
            return error != null;
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
        atHead = true;
        if ( goLatch.getCount() != regions.length() ) {
            checkForClosed();
        }
        
        while ( !goLatch.await(wait, TimeUnit.MILLISECONDS) ) {
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
            if ( size >= 0 ) {
                this.close(record.getLsn());
            }
        }
    }

    @Override
    public void exceptionThrown(Exception exp) {
      CommitList chain = null;
        synchronized (guard) {
            error = exp;
            guard.notifyAll();
            if ( next != null ) {
              chain = next;
            }
        }
        if ( chain != null ) {
          chain.exceptionThrown(exp);
        }
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        this.waitForWrite();
        return null;
    }

    @Override
    public Void get(long time, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
        this.waitForWrite(tu.convert(time, TimeUnit.MILLISECONDS));
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return (goLatch.getCount() == regions.length() || this.isWritten() || this.isError() );
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
