/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 *
 * @author mscott
 */
public class StackingCommitList implements CommitList {

    private final LogRecord[] regions;
// set at construction    
    private final long baseLsn;
//  half synchronized
    private volatile boolean syncing = false;
//  these are synchronized     
    private long endLsn;
//    private long lowestLsn;
    private boolean closed = false;
    private final CompletableFuture<Void> written = new CompletableFuture<>();
    private int count = 0;
    
    private final Object guard = new Object();
    private volatile CommitList next;
    private final int wait;

    public StackingCommitList(long startLsn, int maxSize, int wait) {
        baseLsn = startLsn;
        endLsn = startLsn-1;
        regions = new LogRecord[maxSize];
        this.wait = wait;
    }

     @Override
   public boolean append(LogRecord record, boolean sync) {
        assert (record.getLsn() >= baseLsn);

        if (record.getLsn() >= regions.length + baseLsn) {
            return false;
        }

        regions[(int) (record.getLsn() - baseLsn)] = record;

        if (!countRecord(record.getLsn(),sync)) {
            regions[(int) (record.getLsn() - baseLsn)] = null; //  just to be clean;
            return false;
        }
        
        return true;
    }
 
    @Override
    public CommitList next() {
        if (next == null) {
            synchronized (this) {
                if ( !closed ) {
                    closed = true;
                }
                if (next == null) {
                    next = create(endLsn + 1);
                }
            }
        }
        return next;
    }
    
    public CommitList create(long nextLsn) {
        return new StackingCommitList( nextLsn, regions.length, wait);
    }

    @Override
    public boolean isEmpty() {
        return ( endLsn < baseLsn );
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
    private synchronized boolean countRecord(long lsn, boolean sync) {
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
        if ( sync ) {
            this.syncing = true;
            this.notify();
        }
        
        return true;
    }
    
    @Override
    public synchronized boolean close(long lsn) {
        if ( lsn <= endLsn ) {
            closed = true;
            this.notify();
        }

        return closed;
    }

    @Override
    public boolean isSyncRequested() {
        return syncing;
    }

    @Override
    public void written() {
        written.complete(null);
    }
    
    
    @Override
    public void exceptionThrown(Exception exp) {
        CommitList chain;
        written.completeExceptionally(exp);
        synchronized (guard) {
            chain = next;
        }
        if ( chain != null ) {
          chain.exceptionThrown(exp);
        }
    }

    @Override
    public synchronized void waitForContiguous() throws InterruptedException {
        boolean timedout = false;
        if ( count > 0 && !closed ) {
            this.close(baseLsn + count - 1);
        }
        
        while ((!closed && count != regions.length) || (closed && count != endLsn - baseLsn + 1)) {
            this.wait(wait);
            if ( timedout ) {
                if ( count > 0 ) {
                    this.close(baseLsn + count - 1);
                    timedout = false;
                }
            } else {
                timedout = true;
            }
            
        }
        if (count != regions.length && count != endLsn - baseLsn + 1) {
            throw new AssertionError();
        }
    }

    @Override
    public Future<Void> getWriteFuture() {
        return written;
    }
    
//  iterator interface
    @Override
    public Iterator<LogRecord> iterator() {
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
