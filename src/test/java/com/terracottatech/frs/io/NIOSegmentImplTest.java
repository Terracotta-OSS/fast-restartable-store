/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author mscott
 */
public class NIOSegmentImplTest {

    NIOStreamImpl stream;
    NIOSegmentImpl tester;
    File workarea;
    
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    public NIOSegmentImplTest() {
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
            stream = new NIOStreamImpl(workarea, (10 * 1024 * 1024));
            tester = (NIOSegmentImpl)stream.append();
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of append method, of class NIOSegmentImpl.
     */
    @Test
    public void testAppend() throws Exception {
        System.out.println("append");
        Chunk c = new AbstractChunk() {

            @Override
            public ByteBuffer[] getBuffers() {
                ByteBuffer[] list = new ByteBuffer[2];
                list[0] = ByteBuffer.allocate(30);
                list[1] = ByteBuffer.allocate(10);
                return list;
            }

            @Override
            public long length() {
                return 40;
            }
        };
        try {
            long result = tester.append(c);
            assertEquals(60l, result);  //  length of these bytes plus 20 bytes of header for LogRegions
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
    }

    /**
     * Test of close method, of class NIOSegmentImpl.
     */
    @Test
    public void testClose() throws Exception {
        System.out.println("close");
        tester.close();
    }

    /**
     * Test of fsync method, of class NIOSegmentImpl.
     */
    @Test
    public void testFsync() throws Exception {
        System.out.println("fsync");
        tester.fsync();
    }

    /**
     * Test of isClosed method, of class NIOSegmentImpl.
     */
    @Test
    public void testIsClosed() {
        System.out.println("isClosed");
        try {
            assertEquals(false, tester.isClosed());
            tester.close();
            boolean expResult = true;
            boolean result = tester.isClosed();
            assertEquals(expResult, result);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
    }

    /**
     * Test of iterator method, of class NIOSegmentImpl.
     */
    @Test @Ignore
    public void testIterator_Direction() {
        System.out.println("iterator");
    }

    /**
     * Test of length method, of class NIOSegmentImpl.
     */
    @Test
    public void testLength() throws Exception {
        System.out.println("length");
        long expResult = 26L;  //  header length
        long result = tester.length();
        assertEquals(expResult, result);
    }

    /**
     * Test of remains method, of class NIOSegmentImpl.
     */
    @Test
    public void testRemains() throws Exception {
        System.out.println("remains");
        long expResult = (10L*1024*1024) - 26L;  //  test append new file minus header
        long result = tester.remains();
        assertEquals(expResult, result);
    }

    /**
     * Test of iterator method, of class NIOSegmentImpl.
     */
    @Test @Ignore
    public void testIterator_0args() {
        System.out.println("iterator");
    }
}
