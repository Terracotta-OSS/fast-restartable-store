/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.SimpleLogManager;
import com.terracottatech.frs.log.StackingLogRegion;
import com.terracottatech.frs.log.LogRecord;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author mscott
 */
public class NIOManagerTest {
    
    NIOManager manager;
    long lsn = 0;
    File workarea;
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    public NIOManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() throws IOException {
            workarea = folder.newFolder();
            System.out.println(workarea.getAbsolutePath());
            manager = new NIOManager(workarea.getAbsolutePath(),(10*1024*1024));
            
    }
    
    @After
    public void tearDown() throws IOException {
        manager.dispose();
    }

    /**
     * Test of append method, of class IOManagerImpl.
     */
    @Test
    public void testAppend() throws IOException {
        System.out.println("append");
        int count = (int)(Math.random() * 100);
        System.out.format("writing %d log regions\n",count);
        long tb = 0;
        int lastSync = -1;
        long lastLen = 0;
        long total = System.nanoTime();

        for ( int x=0;x<count;x++) {
            StackingLogRegion test = createLogRegion();
            long start = System.nanoTime();
            tb +=manager.write(test);
            System.out.format("Log Region write time: %.6f sec\n", (System.nanoTime() - start) * 1e-9);
            
            if ( Math.random() * 10 < 1 ) {
                start = System.nanoTime();
                manager.sync();
                System.out.format("Log Stream sync time: %.6f sec count: %d length: %dk \n", 
                        (System.nanoTime() - start) * 1e-9,
                        x-lastSync,
                        (tb - lastLen)/1024
                );
                lastSync = x;
                lastLen = tb;
            }
        }

        System.out.format("bytes written: %.3fM in %.6f sec = %.3f bytes/sec\n",tb/(1024d*1024d),
                (System.nanoTime() - total)*1e-9,
                tb/((System.nanoTime() - total)*1e-9)
                );
    }
    
    @Test 
    public void testMTAppend() {
        System.out.println("MT append");
        final int count = (int)(Math.random() * 10) + 10;
         System.out.format("forming %d user threads\n",count);
       final SimpleLogManager lm = new SimpleLogManager(manager);
       lm.startup();
       
        ThreadGroup tg = new ThreadGroup("tester");
        final CountDownLatch wait = new CountDownLatch(count);
        long total = System.nanoTime();

        for (int x=0;x<count;x++) {
            new Thread(tg,new Runnable() {

                @Override
                public void run() {
                    int spins = (int)(Math.random() * 1000);
                     System.out.format("pushing %d log records\n",spins);
                    for (int x=0;x<spins;x++) {
                        try {
                            long start = System.nanoTime();
                            boolean sync = Math.random() * 10 < 1;
                            TestLogRecord lr = new TestLogRecord();
                            Future<Void> wio = ( sync ) ?  lm.appendAndSync(lr) : lm.append(lr);

                            if ( sync ) {
                                wio.get();
                                System.out.format("Log Stream sync time: %.6f sec \n", 
                                        (System.nanoTime() - start) * 1e-9);
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
        System.out.format("bytes written: %.3fM in %.6f sec = %.3f bytes/sec\n",tb/(1024d*1024d),
                (System.nanoTime() - total)*1e-9,
                tb/((System.nanoTime() - total)*1e-9)
                );
    }
    
    private StackingLogRegion createLogRegion() throws IOException {
        StackingLogRegion region = new StackingLogRegion(true, lsn, 100);
        int count = (int)(Math.random() * 100) + 3;
        for ( int x=0;x<count;x++) {
            LogRecord lr = new TestLogRecord();
            lr.updateLsn(lsn++);
            region.append(lr);
        }
        boolean closed = region.close(false);
        return region;
    }

    /**
     * Test of reader method, of class IOManagerImpl.
     */
//    @Test
//    public void testReader() {
//        System.out.println("reader");
//        LogRegionFactory<T> as = null;
//        IOManagerImpl instance = new IOManagerImpl();
//        Iterator expResult = null;
//        Iterator result = instance.reader(as);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
}
