/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.AbstractChunk;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.nio.NIOSegmentImpl;
import com.terracottatech.frs.io.nio.NIOStreamImpl;
import com.terracottatech.frs.util.JUnitTestFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.*;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author mscott
 */
public class NIOSegmentImplTest {

    NIOStreamImpl stream;
    File workarea;
    
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();

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
            stream = new NIOStreamImpl(workarea, (1 * 1024 * 1024));
            stream.setMinimumMarker(100);
            stream.setMarker(100);
            stream.setMaximumMarker(100);
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
            long result = stream.append(c);
            assertEquals(72l, result);  //  length of these bytes plus 32 bytes of header for LogRegions
    }

    /**
     * Test of close method, of class NIOSegmentImpl.
     */
    @Test
    public void testClose() throws Exception {
        System.out.println("close");
        stream.close();
    }

    /**
     * Test of fsync method, of class NIOSegmentImpl.
     */
    @Test
    public void testFsync() throws Exception {
        System.out.println("fsync");
        stream.sync();
    }

    /**
     * Test of isClosed method, of class NIOSegmentImpl.
     */
    @Test
    public void testIsClosed() throws Exception {
        System.out.println("isClosed");

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
    }

    /**
     * Test of remains method, of class NIOSegmentImpl.
     */
    @Test
    public void testRemains() throws Exception {
        System.out.println("remains");
        long expResult = (1L*1024*1024) - 26L;  //  test append new file minus header
    }

    /**
     * Test of iterator method, of class NIOSegmentImpl.
     */
    @Test @Ignore
    public void testIterator_0args() {
        System.out.println("iterator");
    }
}
