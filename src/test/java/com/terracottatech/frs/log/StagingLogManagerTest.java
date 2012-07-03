/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.IOStatistics;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import junit.framework.Assert;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.mockito.Mockito.*;
import static org.junit.Assert.fail;
import org.junit.Ignore;

/**
 * @author mscott
 */
public class StagingLogManagerTest {

    private static final long LOG_REGION_WRITE_TIMEOUT = 10;
    private DummyIOManager ioManager;
    private LogManager logManager;
    private boolean startThrowing = false;
    

    @Before
    public void setUp() throws Exception {
        ioManager = spy(new DummyIOManager());
        logManager = new StagingLogManager(ioManager);
    }

    /**
     * Test of appendAndSync method, of class StagingLogManager.
     */
    @Test
    public void testAppendAndSync() throws Exception {
        logManager.startup();
        LogRecord record = newRecord(-1);
        Future<Void> f = logManager.appendAndSync(record);
        f.get(LOG_REGION_WRITE_TIMEOUT, SECONDS);
        verify(ioManager).write(any(Chunk.class),any(Long.class));
    }

    /**
     * Test of append method, of class StagingLogManager.
     */
    @Test
    public void testAppend() throws Exception {
        logManager.startup();
        for (long i = 100; i < 200; i++) {
            LogRecord record = spy(newRecord(-1));
            logManager.append(record);
            verify(record).updateLsn(i);
        }
        logManager.shutdown();
        verify(ioManager, atLeastOnce()).write(any(Chunk.class),any(Long.class));
    }
    
    @Test 
    public void testAppendException() {
        logManager.startup();
        startThrowing = true;

        try {
            for (long i = 100; i < 100000000; i++) {
                LogRecord record = spy(newRecord(-1));
                logManager.append(record);
                verify(record).updateLsn(i);
            }
            fail();
        } catch ( LogWriteError err ) {
            System.out.println("caught log write error");
        }
        logManager.shutdown();
    }

    @Test
    public void testMultiThreadedAppendAndSync() throws Exception {
        logManager.startup();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        List<Callable<Void>> runnables = new ArrayList<Callable<Void>>();
        Random r = new Random();
        final AtomicInteger syncs = new AtomicInteger();
        for (int i = 0; i < 1000; i++) {
            if (r.nextInt(100) < 25) {
                runnables.add(new Callable<Void>() {

                    @Override
                    public Void call() {
                        try {
                            logManager.appendAndSync(newRecord(-1)).get();
                            syncs.incrementAndGet();
                            return null;
                        } catch (Exception exp) {
                            throw new RuntimeException(exp);
                        }
                    }
                });
            } else {
                runnables.add(new Callable<Void>() {

                    @Override
                    public Void call() {
                        try {
                            logManager.append(newRecord(-1)).get();
                            return null;
                        } catch (Exception exp) {
                            throw new RuntimeException(exp);
                        }
                    }
                });
            }
        }
        for (Future<Void> future : executorService.invokeAll(runnables)) {
            future.get();
        }
        executorService.shutdown();

        // Some of the syncs can wind up overlapping, so let's say at least 50% of them
        // can trigger a new write.
        verify(ioManager, atLeast(syncs.get() / 10)).write(any(Chunk.class),any(Long.class));
    }

    /**
     * Test of reader method, of class StagingLogManager.
     */
    @Test
    public void testReader() throws Exception {
        long lsn = 100;
        for (int i = 0; i < 10; i++) {
            List<LogRecord> records = new ArrayList<LogRecord>();
            for (int j = 0; j < 100; j++) {
                LogRecord record = newRecord(-1);
                record.updateLsn(lsn);
                lsn++;
                records.add(record);
            }
            ioManager.write(new LogRegionPacker(Signature.ADLER32).pack(records),lsn-1);
        }
        logManager.startup();

        long expectedLsn = 1099;
        Iterator<LogRecord> i = logManager.reader();
        while (i.hasNext()) {
            LogRecord record = i.next();
//            assertThat(record.getLowestLsn(), is(0L));
            assertThat(record.getLsn(), is(expectedLsn));
            expectedLsn--;
        }
        assertThat(expectedLsn, is(99L));
    }

    @Test
    public void testSlowPuts() throws Exception {
        long lsn = 100;
        for (int i = 0; i < 10; i++) {
            List<LogRecord> records = new ArrayList<LogRecord>();
            for (int j = 0; j < 10; j++) {
                LogRecord record = newRecord(-1);
                record.updateLsn(lsn);
                lsn++;
                records.add(record);
            }
            ioManager.write(new LogRegionPacker(Signature.ADLER32).pack(records),lsn-1);
        }
        logManager.startup();

        long expectedLsn = 199;
        Iterator<LogRecord> i = logManager.reader();
        while (i.hasNext()) {
            LogRecord record = i.next();
//            assertThat(record.getLowestLsn(), is(0L));
            assertThat(record.getLsn(), is(expectedLsn));
            expectedLsn--;
            Thread.sleep(100);
            System.out.println("got " + record.getLsn());
        }
        assertThat(expectedLsn, is(99L));
    }  
    
    @Test
    public void testShutdownNoStartup() throws Exception {
        logManager.shutdown();
        verify(ioManager, atLeastOnce()).close();
    }
    
    @Test
    public void testDoubleShutdown() throws Exception {
        logManager.startup();
        logManager.shutdown();
        logManager.shutdown();
        verify(ioManager, atLeastOnce()).close();
    }
    
    
    @Test
    public void testReaderDeath() throws Exception {
        long lsn = 100;
        for (int i = 0; i < 10; i++) {
            List<LogRecord> records = new ArrayList<LogRecord>();
            for (int j = 0; j < 10; j++) {
                LogRecord record = newRecord(-1);
                record.updateLsn(lsn++);
                records.add(record);
            }
            ioManager.setMinimumMarker(99);
            ioManager.write(new LogRegionPacker(Signature.ADLER32).pack(records),lsn-1);
        }
        
        ioManager.dieOnRead(); // dies with 10 records left to read
        try {
            logManager.startup();
            Assert.fail("expected reader exception");
        } catch ( Throwable t ) {
//   receovery may have died already, expected
            t.printStackTrace();
        }

        long expectedLsn = 199;
        Iterator<LogRecord> i = logManager.reader();
        try {
            while (i.hasNext()) {
                LogRecord record = i.next();
    //            assertThat(record.getLowestLsn(), is(0L));
                assertThat(record.getLsn(), is(expectedLsn--));
                System.out.println("got " + record.getLsn());
            }
            Assert.fail("expected reader exception");
       } catch ( Throwable t ) {
            t.printStackTrace();
        }
        assertThat(expectedLsn, not(99L));
    }
    
    @Test
    public void testLowestLsn() throws Exception {
        logManager.startup();
        
        assertThat(logManager.lowestLsn(), greaterThan(99L));
    }
    
    
    @Test
    public void testSlowReader() throws Exception {
        long lsn = 100;
        for (int i = 0; i < 10; i++) {
            List<LogRecord> records = new ArrayList<LogRecord>();
            for (int j = 0; j < 10; j++) {
                LogRecord record = newRecord(-1);
                record.updateLsn(lsn);
                lsn++;
                records.add(record);
            }
            ioManager.write(new LogRegionPacker(Signature.ADLER32).pack(records),lsn-1);
        }
        ioManager.slowReads();
        logManager.startup();

        long expectedLsn = 199;
        Iterator<LogRecord> i = logManager.reader();
        while (i.hasNext()) {
            LogRecord record = i.next();
//            assertThat(record.getLowestLsn(), is(0L));
            assertThat(record.getLsn(), is(expectedLsn));
            expectedLsn--;
            System.out.println("got " + record.getLsn());
        }
        assertThat(expectedLsn, is(99L));
    }      

    private LogRecord newRecord(long lowest) {
        return new LogRecordImpl(lowest, new ByteBuffer[0], mock(LSNEventListener.class));
    }

    private class DummyIOManager implements IOManager {

        private final Deque<Chunk> chunks = new LinkedList<Chunk>();
        private long min = 99;
        private long current = 99;
        private boolean slowReads = false;
        private boolean dieOnRead = false;

        @Override
        public long write(Chunk region, long lsn) throws IOException {
            if ( startThrowing ) throw new IOException();
            current = lsn;
            chunks.push(region);
            return 0;
        }
        
    @Override
    public void setMinimumMarker(long lsn) throws IOException {
        min = lsn;
    }

    @Override
    public long getCurrentMarker() throws IOException {
        return current;
    }

    @Override
    public long getMinimumMarker() throws IOException {
        return min;
    }
    
    public void slowReads() {
        slowReads = true;
    }
    
    public void dieOnRead() {
        dieOnRead = true;
    }

        @Override
        public IOStatistics getStatistics() throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Future<Void> clean(long timeout) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public Chunk read(Direction dir) throws IOException {
            if ( slowReads ) {
                try {
                    Thread.sleep(100);
                } catch ( InterruptedException ie ) {
                    throw new RuntimeException(ie);
                }
            }
            if ( dieOnRead && chunks.size() == 10 ) {
                throw new IOException("!!!!!corrupted!!!!");
            }
            
            if (chunks.isEmpty()) {
                return null;
            }
            return chunks.pop();
        }

        @Override
        public long seek(long lsn) throws IOException {
            return 0;
        }

        @Override
        public void sync() throws IOException {
        }

        @Override
        public void close() throws IOException {
        }
    }
}
