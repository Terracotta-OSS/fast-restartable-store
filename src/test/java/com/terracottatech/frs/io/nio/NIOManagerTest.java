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
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.GlobalFilters;
import com.terracottatech.frs.io.MaskingBufferSource;
import com.terracottatech.frs.io.SplittingBufferSource;
import com.terracottatech.frs.io.TimebombFilter;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.log.*;
import com.terracottatech.frs.util.JUnitTestFolder;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import org.junit.BeforeClass;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author mscott
 */
public class NIOManagerTest {
    private NIOManager manager;
    private long lsn = Constants.FIRST_LSN;
    private File workArea;
    private Configuration config;
    private static BufferSource src;
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();
    
    @BeforeClass
    public static void setupClass() throws Exception {
      src = new MaskingBufferSource(new SplittingBufferSource(16, 8 * 1024 * 1024));
    }
    
    @Before
    public void setUp() throws IOException {
        workArea = folder.newFolder();
        System.out.println(workArea.getAbsolutePath());
        Properties props = new Properties();
        props.setProperty("io.nio.bufferBuilder", "com.terracottatech.frs.io.SimulatingBufferBuilder");
        props.setProperty("io.nio.segmentSize", Long.toString(1L * 1024 * 1024));
        props.setProperty("io.nio.memorySize", Long.toString(10L * 1024 * 1024));
        try {
            props.store(new FileWriter(new File(workArea,"frs.properties")), null);
        } catch ( IOException io ) {

        }
        config = Configuration.getConfiguration(workArea);
        try {
          manager = new NIOManager(config, src);
        } catch ( Throwable t ) {
          t.printStackTrace();
        }
        manager.setMinimumMarker(Constants.FIRST_LSN);
    }
    
    @After
    public void tearDown() throws IOException {
        try {
            manager.close();
        } catch ( IOException ioe ) {
        
        }
        manager = null;
        System.gc();
    }
    
    @Test
    public void testClose() throws IOException {
        manager.close();
        assert(manager.isClosed());
    }
        
    @Test
    public void testDoubleClose() throws IOException {
        manager.close();
        manager.close();
        assert(manager.isClosed());
    }

    /**
     * Test of append method, of class IOManagerImpl.
     */
    @Test
    public void testAppendPerformance() throws IOException {
        System.out.println("append");
        int count = (int)(Math.random() * 100);
        System.out.format("writing %d log regions\n",count);
        long tb = 0;
        int lastSync = -1;
        long lastLen = 0;
        long total = System.nanoTime();
        long marker = Constants.FIRST_LSN;
        for ( int x=0;x<count;x++) {
            Chunk test = createLogRegion();
            long start = System.nanoTime();
            tb +=manager.write(test,marker+=100);
//            System.out.format("Log Region write time: %dms\n", NANOSECONDS.toMillis(System.nanoTime() - start));
            
            if ( Math.random() * 10 < 1 ) {
                start = System.nanoTime();
                manager.sync();
//                System.out.format("Log Stream sync time: %dms count: %d length: %dk \n",
//                       NANOSECONDS.toMillis(System.nanoTime() - start),
//                        x-lastSync,
//                        (tb - lastLen)/1024
//                );
                lastSync = x;
                lastLen = tb;
            }
        }

        System.out.format("bytes written: %.3fM in %dms = %.3f MB/sec\n",
            tb / (1024d * 1024d),
            NANOSECONDS.toMillis(System.nanoTime() - total),
            (tb / (1024d * 1024d)) / ((System.nanoTime() - total) * 1e-9)
        );
    }
    
    @Test 
    public void testAtomicMT() {
        System.out.println("Atomic MT append");
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new AtomicCommitList( lsn, 100, 20),manager, src);
        testMTAppend(lm);
    }

    @Test
    public void testStackingMT() {
        System.out.println("Stacking MT append");
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new StackingCommitList( lsn, 100, 10),manager, src);
        testMTAppend(lm);
    }
    
     @Test
    public void testWriteSuspend() throws Exception {
        System.out.println("write then suspend");
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new StackingCommitList( lsn, 100, 20),manager, src);
        lm.startup();
        lm.shutdown();
    }     
    
        
    private void testMTAppend(final StagingLogManager lm) {
        int count = 20;
        lm.startup();
        System.out.format("forming %d user threads\n",count);
        ThreadGroup tg = new ThreadGroup("tester");
        final CountDownLatch wait = new CountDownLatch(count);
        long total = System.nanoTime();

        for (int x=0;x<count;x++) {
            new Thread(tg,new Runnable() {

                @Override
                public void run() {
                    int spins = 100;
//                     System.out.format("pushing %d log records\n",spins);
                     try {
                    for (int x=0;x<spins;x++) {
                        try {
                            long start = System.nanoTime();
                            int sync = (x%9);
                            LogRecord lr = new DummyLogRecord(100,1024);
                            if ( sync == 1 ) {
                                lm.appendAndSync(lr).get();
                            } else {
                                lm.append(lr);
                            }
                        } catch ( IOException ioe ) {
                            throw new AssertionError(ioe);
                        } catch ( InterruptedException ie ) {
                            throw new AssertionError(ie);
                        } catch ( ExecutionException e ) {
                            throw new AssertionError(e);
                        }
                    }
                     } finally {
                    wait.countDown();
                     }
                }
                
            }).start();
        }
        
        try {
            wait.await();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        lm.shutdown();
    }
    
    private Chunk createLogRegion() throws IOException {
        ArrayList<LogRecord> items = new ArrayList<LogRecord>();
        int count = (int)(Math.random() * 100) + 3;
        
        return new WrappingChunk(ByteBuffer.allocate(1024));
    }

    /**
     * Test of reader method, of class IOManagerImpl.
     */
    @Test
    public void testReader() throws IOException {
        System.out.println("reader");
       StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new AtomicCommitList(Constants.FIRST_LSN, 100, 20), manager, src);
       lm.startup();
       for (int x=0;x<1000;x++) {
           DummyLogRecord lr1 = new DummyLogRecord(100,1024);
           lm.append(lr1);
       }

       lm.shutdown();
       
       manager = new NIOManager(config, src);
       lm = new StagingLogManager(Signature.ADLER32, new AtomicCommitList(Constants.FIRST_LSN, 100, 20), manager, src);
       
       
       long lsn = -1;
       Iterator<LogRecord> logs = lm.startup();
       while ( logs.hasNext() ) {
           LogRecord record = logs.next();
           if ( lsn > 0 ) {
             assertEquals(lsn-1 ,record.getLsn());
           };
           lsn = record.getLsn();
       }
    }

    /**
     * Ensures that {@link NIOManager#scan(long)} completes after waiting for write completion.
     */
    @Test
    public void testScan() throws Exception {
        final int recordCount = 1000;
        final long triggerMarker = Constants.FIRST_LSN + recordCount - 10;

        /*
         * Inject a "spied" NIOStreamImpl into the NIOManager instance so
         * the 'waitForMarker' method activity can be watched.
         */
        final Field backendField = NIOManager.class.getDeclaredField("backend");
        backendField.setAccessible(true);
        NIOStreamImpl backend = (NIOStreamImpl)backendField.get(manager);
        backend = spy(backend);
        backendField.set(manager, backend);

        final CountDownLatch latch = new CountDownLatch(1);
        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocationOnMock) throws Throwable {
                final Long marker = (Long)invocationOnMock.getArguments()[0];
                if (marker == triggerMarker) {
                    System.out.format("waitForWriteOf(%s)%n", marker);
                    System.out.flush();
                    latch.countDown();          // Release the record loading thread
                }
                return invocationOnMock.callRealMethod();
            }
        }).when(backend).waitForWriteOf(anyLong());

        final StagingLogManager lm =
            new StagingLogManager(Signature.ADLER32, new AtomicCommitList(Constants.FIRST_LSN, 100, 20), manager, src);
        lm.startup();

        /*
         * Load records into log manager pausing 1/2 way through to wait for a call to
         * NIOStreamImpl.waitForMarker for the trigger marker value.
         */
        final Thread loadingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int x = 0; x < recordCount; x++) {
                    try {
                        DummyLogRecord record = new DummyLogRecord(100, 1024);
                        if (x == recordCount - 1) {
                            final Future<Void> future = lm.appendAndSync(record);
                            future.get();
                        } else if (x == recordCount / 2) {
                            final Future<Void> future = lm.appendAndSync(record);
                            future.get();
                            System.out.println("Record loading suspended");
                            latch.await();          // Wait for a call to NIOStreamImpl.waitForMarker(triggerMarker)
                        } else {
                            lm.append(record);
                        }
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    } catch (ExecutionException e) {
                        throw new AssertionError(e);
                    }
                }
            }
        });
        loadingThread.setDaemon(true);
        loadingThread.start();

        assertNotNull(manager.scan(Constants.FIRST_LSN));
        assertThat(lm.currentLsn(), lessThan(triggerMarker));
        assertThat(latch.getCount(), not(equalTo(0L)));
        assertNotNull(manager.scan(triggerMarker));

        loadingThread.join();
        lm.shutdown();
    }

    @Test
    public void testTimeBomb() throws Exception {
        GlobalFilters.addFilter(new TimebombFilter(1, TimeUnit.SECONDS));
        int count =0;
        DecimalFormat df = new DecimalFormat("0000000000");
        long marker = 100;
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.wrap(df.format(count++).getBytes())),marker+=100);
                if ( count % 10000 == 0 ) manager.sync();
            }
        } catch ( IOException ioe ) {
            System.err.println(ioe.toString() + " " + ioe.getMessage());
//            ioe.printStackTrace();
        } 
        System.out.println("written count: " + Integer.toString(count));
        
        manager.seek(0);
        manager.close();
        
        while ( Thread.interrupted() ) {};
        
        manager = new NIOManager(config, src);
        manager.seek(-1);
        Chunk c = manager.read(Direction.REVERSE);
        int check = 0;
        byte[] buf = new byte[10];
        while ( c != null ) {
            check += 1;
            c = manager.read(Direction.REVERSE);
            if ( c != null ) {
                c.get(buf);
//                System.out.println(new String(buf));
            }
        }
        System.out.println("count: " + count + " check: " + check);
    }
}
