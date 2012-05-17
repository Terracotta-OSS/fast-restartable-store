/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.GlobalFilters;
import com.terracottatech.frs.io.TimebombFilter;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.log.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 *
 * @author mscott
 */
public class NIOManagerTest {
    private NIOManager manager;
    private long lsn = 100;
    private File workArea;
    private Configuration config;
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    @Before
    public void setUp() throws IOException {
            workArea = folder.newFolder();
            System.out.println(workArea.getAbsolutePath());
            Properties props = new Properties();
            props.setProperty("io.nio.bufferBuilder", "com.terracottatech.frs.io.SimulatingBufferBuilder");
            props.setProperty("io.nio.segmentSize", Integer.toString(1024 * 1024));
            props.setProperty("io.nio.memorySize", Integer.toString(10 * 1024 * 1024));
            try {
                props.store(new FileWriter(new File(workArea,"frs.properties")), null);
            } catch ( IOException io ) {
                
            }
            config = Configuration.getConfiguration(workArea);
            manager = new NIOManager(config);
    }
    
    @After
    public void tearDown() throws IOException {
        manager.close();
        manager = null;
        System.gc();
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
            Chunk test = createLogRegion();
            long start = System.nanoTime();
            tb +=manager.write(test);
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
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new AtomicCommitList( lsn, 100, 20),manager);
        testMTAppend(lm);
    }

    @Test
    public void testStackingMT() {
        System.out.println("Stacking MT append");
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new StackingCommitList( lsn, 100, 10),manager);
        testMTAppend(lm);
    }
    
     @Test
    public void testWriteSuspend() throws Exception {
        System.out.println("write then suspend");
        final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new StackingCommitList( lsn, 100, 20),manager);
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
                     System.out.format("pushing %d log records\n",spins);
                     try {
                    for (int x=0;x<spins;x++) {
                        try {
                            long start = System.nanoTime();
                            int sync = (x%9);
                            LogRecord lr = new DummyLogRecord(100,1024);
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
       final StagingLogManager lm = new StagingLogManager(Signature.ADLER32, new AtomicCommitList(100l, 100, 20), manager);
       lm.startup();
       for (int x=0;x<1000;x++) {
            DummyLogRecord lr1 = new DummyLogRecord(100,1024);
           lm.append(lr1);
       }

       lm.shutdown();
       
       lm.startup();
       
       long lsn = -1;
       Iterator<LogRecord> logs = lm.reader();
       while ( logs.hasNext() ) {
           LogRecord record = logs.next();
           System.out.println(record.getLsn());
           if ( lsn > 0 ) assert(lsn-1 == record.getLsn());
           lsn = record.getLsn();
       }
    }
    
    @Test
    public void testTimeBomb() throws Exception {
        GlobalFilters.addFilter(new TimebombFilter(1, TimeUnit.SECONDS));
        int count =0;
        DecimalFormat df = new DecimalFormat("0000000000");
        try {
            while ( true ) {
                manager.write(new WrappingChunk(ByteBuffer.wrap(df.format(count++).getBytes())));
                if ( count % 10000 == 0 ) manager.sync();
            }
        } catch ( IOException ioe ) {
            ioe.printStackTrace();
        }
        System.out.println(Integer.toString(count));
        
        manager.seek(0);
        manager.close();
        
        while ( Thread.interrupted() ) {};
        
        manager = new NIOManager(config);
        manager.seek(-1);
        Chunk c = manager.read(Direction.REVERSE);
        int check = 0;
        byte[] buf = new byte[10];
        while ( c != null ) {
            check += 1;
            c = manager.read(Direction.REVERSE);
            if ( c != null ) {
                c.get(buf);
                System.out.println(new String(buf));
            }
        }
        System.out.println("count: " + count + " check: " + check);
    }
}
