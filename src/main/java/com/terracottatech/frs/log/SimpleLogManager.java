/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.IOManager;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Simple LogManager with a single daemon thread for IO operations
 * 
 * 
 * @author mscott
 */
public class SimpleLogManager implements LogManager,Runnable {
    
    Thread daemon;
    volatile StackingLogRegion current_region;
    AtomicLong currentLsn = new AtomicLong(100);
    AtomicLong highestOnDisk = new AtomicLong(0);
    IOManager io;
    final int max_region = 100;
    AtomicBoolean alive = new AtomicBoolean(true);
    long tb = 0;
    
    final static Future<Void> _dummy_future = new Future() {

        @Override
        public boolean cancel(boolean bln) {
            return false;
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }
        
    };
    

    public SimpleLogManager(IOManager io) {
        this.daemon = new Thread(this);
        this.daemon.setDaemon(true);
        this.io = io;
        current_region = new StackingLogRegion(true, currentLsn.get(), max_region);
    }
    
     //  TODO:  re-examine when more runtime context is available.
    public void run() {
        while ( alive.get() || currentLsn.get()-1 != highestOnDisk.get() ) {
            StackingLogRegion old_region = current_region;
            try {
                if ( !alive.get() ) old_region.close(false);
                old_region.waitForContiguous();
                
                current_region = old_region.next();
                
                tb += io.write(old_region);
                if ( old_region.isSyncing() ) {
                    io.sync();
                }
                highestOnDisk.set(old_region.getEndLsn());
                old_region.written();
            } catch ( IOException ioe ) {
                throw new AssertionError(ioe);
            } catch ( InterruptedException ie ) {
                if ( alive.get() ) throw new AssertionError(ie);
                else old_region.close(false);
            }        
        }
    }
    //  TODO:  re-examine when more runtime context is available.
    public void startup() {
        this.daemon.start();
    }

    //  TODO:  re-examine when more runtime context is available.
    public void shutdown() {
        alive.set(false);
        current_region.close(false);
        try {
            daemon.join();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        assert(currentLsn.get()-1 == highestOnDisk.get());
    }
    
    public long totalBytes() {
        return tb;
    }
    
    private StackingLogRegion commonAppend(LogRecord record) {
//        if ( !alive.get() ) throw new RuntimeException("shutting down");
       long lsn = currentLsn.getAndIncrement();
        record.updateLsn(lsn);
        
        StackingLogRegion mine = current_region;
                
        while ( !mine.append(record) ) {
            mine.close(false);
            mine = mine.next();
        }
        
        return mine;
    }
    
    @Override
    public Future<Void> appendAndSync(LogRecord record) {
        final StackingLogRegion mine = commonAppend(record);
        
        mine.close(true);
                
        return new Future<Void>() {

            @Override
            public boolean cancel(boolean bln) {
                //  can we cancel?
                return false;
            }
            
            @Override
            public Void get() throws InterruptedException, ExecutionException {
                mine.waitForWrite();
                return null;
            }

            @Override
            public Void get(long l, TimeUnit tu) throws InterruptedException, ExecutionException, TimeoutException {
                mine.waitForWrite(tu.convert(l,tu.MILLISECONDS));
                return null;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return mine.isWritten();
            }
            
        };        
    }

    @Override
    public Future<Void> append(LogRecord record) {
        
        StackingLogRegion mine = commonAppend(record);
                
        return _dummy_future;
    }

    @Override
    public Iterator<LogRecord> reader() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    
    
}
