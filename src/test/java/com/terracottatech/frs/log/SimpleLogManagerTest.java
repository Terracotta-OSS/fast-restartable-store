/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.Segment;
import com.terracottatech.frs.io.TestLogRecord;
import com.terracottatech.frs.io.TestLogRegion;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Future;
import org.junit.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 *
 * @author mscott
 */
public class SimpleLogManagerTest {
    
    public SimpleLogManagerTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of run method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testRun() {
        System.out.println("run");
        SimpleLogManager instance = null;
        instance.run();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of startup method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testStartup() {
        System.out.println("startup");
        SimpleLogManager instance = null;
        instance.startup();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of shutdown method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testShutdown() {
        System.out.println("shutdown");
        SimpleLogManager instance = null;
        instance.shutdown();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    } 

    /**
     * Test of totalBytes method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testTotalBytes() {
        System.out.println("totalBytes");
        SimpleLogManager instance = null;
        long expResult = 0L;
        long result = instance.totalBytes();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of appendAndSync method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testAppendAndSync() {
        System.out.println("appendAndSync");
        LogRecord record = null;
        SimpleLogManager instance = null;
        Future expResult = null;
        Future result = instance.appendAndSync(record);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of append method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testAppend() {
        System.out.println("append");
        LogRecord record = mock(LogRecord.class);
        IOManager io = mock(IOManager.class);
        SimpleLogManager mgr = new SimpleLogManager(io);
        mgr.append(record);
        verify(record).updateLsn(0l);
    }

    /**
     * Test of reader method, of class SimpleLogManager.
     */
    @Test @Ignore
    public void testReader() {
        System.out.println("reader");
        SimpleLogManager instance = null;
        Iterator expResult = null;
        Iterator result = instance.reader();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
}
