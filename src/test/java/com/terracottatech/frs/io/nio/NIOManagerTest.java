/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.TestLogRecord;
import com.terracottatech.frs.io.TestLogRegion;
import com.terracottatech.frs.log.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 *
 * @author mscott
 */
public class NIOManagerTest {
    private NIOManager manager;
    private long lsn = 100;
    private File workArea;
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
    public void setUp() throws IOException {
            workArea = folder.newFolder();
            System.out.println(workArea.getAbsolutePath());
            manager = new NIOManager(workArea.getAbsolutePath(), 10 * 1024 * 1024);
    }
    
    @After
    public void tearDown() throws IOException {
        manager.close();
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
        LogRegionPacker packer = new LogRegionPacker(Signature.ADLER32);

        for ( int x=0;x<count;x++) {
            LogRegion test = createLogRegion();
            long start = System.nanoTime();
            tb +=manager.write(packer.pack(test));
            System.out.format("Log Region write time: %dms\n", NANOSECONDS.toMillis(System.nanoTime() - start));
            
            if ( Math.random() * 10 < 1 ) {
                start = System.nanoTime();
                manager.sync();
                System.out.format("Log Stream sync time: %dms count: %d length: %dk \n",
                       NANOSECONDS.toMillis(System.nanoTime() - start),
                        x-lastSync,
                        (tb - lastLen)/1024
                );
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
        final SimpleLogManager lm = new SimpleLogManager(new AtomicCommitList(true, lsn, 100),manager);
        testMTAppend(lm);
    }

    @Test
    public void testStackingMT() {
        System.out.println("Stacking MT append");
        final SimpleLogManager lm = new SimpleLogManager(new StackingCommitList(true, lsn, 100),manager);
        testMTAppend(lm);
    }
    
     @Test
    public void testWriteSuspend() throws Exception {
        System.out.println("write then suspend");
        final SimpleLogManager lm = new SimpleLogManager(new StackingCommitList(true, lsn, 100),manager);
        lm.startup();
        lm.shutdown();
    }     
    
        
    private void testMTAppend(final SimpleLogManager lm) {
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
                     System.out.format("pushing %d log records\n",spins);
                    for (int x=0;x<spins;x++) {
                        try {
                            long start = System.nanoTime();
                            int sync = (x%9);
                            LogRecord lr = new TestLogRecord();
                            if ( sync == 1 ) {
                                lm.appendAndSync(lr).get();
                                System.out.format("Log Stream sync time: %.6f sec \n", 
                                        (System.nanoTime() - start) * 1e-9);
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
                    wait.countDown();
                }
                
            }).start();
        }
        
        try {
            wait.await();
        } catch ( InterruptedException ie ) {
            throw new AssertionError(ie);
        }
        lm.shutdown();
        long tb = lm.totalBytes();
        System.out.format("bytes written: %.3fM in %.6f sec = %.3f MB/sec\n",tb/(1024d*1024d),
                (System.nanoTime() - total)*1e-9,
                (tb/(1024d*1024d))/((System.nanoTime() - total)*1e-9)
                );
    }
    
    private TestLogRegion createLogRegion() throws IOException {
        ArrayList<LogRecord> items = new ArrayList<LogRecord>();
        TestLogRegion region = new TestLogRegion(items);
        int count = (int)(Math.random() * 100) + 3;
        for ( int x=0;x<count;x++) {
            LogRecord lr = new TestLogRecord();
            lr.updateLsn(lsn++);
            items.add(lr);
        }
        return region;
    }

    /**
     * Test of reader method, of class IOManagerImpl.
     */
    @Test
    public void testReader() throws IOException {
        System.out.println("reader");
       final SimpleLogManager lm = new SimpleLogManager(new AtomicCommitList(true, 100l, 100), manager);
       lm.startup();
       for (int x=0;x<1000;x++) {
            TestLogRecord lr1 = new TestLogRecord();
           lm.append(lr1);
       }

       lm.shutdown();
       
       long lsn = -1;
       Iterator<LogRecord> logs = lm.reader();
       while ( logs.hasNext() ) {
           LogRecord record = logs.next();
           System.out.println(record.getLsn());
           if ( lsn > 0 ) assert(lsn-1 == record.getLsn());
           lsn = record.getLsn();
       }
    }
}
