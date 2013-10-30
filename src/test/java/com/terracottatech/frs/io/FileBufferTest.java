/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import org.junit.*;
import static org.junit.Assert.*;

import com.terracottatech.frs.util.JUnitTestFolder;

import static org.mockito.Mockito.mock;

/**
 *
 * @author mscott
 */
public class FileBufferTest {
    
    
    @Rule
    public JUnitTestFolder folder = new JUnitTestFolder();
    
    public FileBufferTest() {
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
     * Test of getTotal method, of class FileBuffer.
     */
    @Test @Ignore
    public void testGetTotal() {
        System.out.println("getTotal");
        FileBuffer instance = null;
        long expResult = 0L;
        long result = instance.getTotal();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getBuffers method, of class FileBuffer.
     */
    @Test @Ignore
    public void testGetBuffers() {
        System.out.println("getBuffers");
        FileBuffer instance = null;
        ByteBuffer[] expResult = null;
        ByteBuffer[] result = instance.getBuffers();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of position method, of class FileBuffer.
     */
    @Test @Ignore
    public void testPosition_0args() {
        System.out.println("position");
        FileBuffer instance = null;
        long expResult = 0L;
        long result = instance.position();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of offset method, of class FileBuffer.
     */
    @Test @Ignore
    public void testOffset() {
        System.out.println("offset");
        FileBuffer instance = null;
        long expResult = 0L;
        long result = instance.offset();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of capacity method, of class FileBuffer.
     */
    @Test @Ignore
    public void testCapacity() {
        System.out.println("capacity");
        FileBuffer instance = null;
        int expResult = 0;
        int result = instance.capacity();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of bufferMove method, of class FileBuffer.
     */
    @Test @Ignore
    public void testBufferMove() {
        System.out.println("bufferMove");
        int src = 0;
        int dest = 0;
        int length = 0;
        FileBuffer instance = null;
        instance.bufferMove(src, dest, length);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of position method, of class FileBuffer.
     */
    @Test @Ignore
    public void testPosition_long() throws Exception {
        System.out.println("position");
        long pos = 0L;
        FileBuffer instance = null;
        FileBuffer expResult = null;
        FileBuffer result = instance.position(pos);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of partition method, of class FileBuffer.
     */
    @Test @Ignore
    public void testPartition() {
        System.out.println("partition");
        int[] pos = null;
        FileBuffer instance = null;
        FileBuffer expResult = null;
        FileBuffer result = instance.partition(pos);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of clear method, of class FileBuffer.
     */
    @Test @Ignore
    public void testClear() {
        System.out.println("clear");
        FileBuffer instance = null;
        instance.clear();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of read method, of class FileBuffer.
     */
    @Test @Ignore
    public void testRead() throws Exception {
        System.out.println("read");
        int count = 0;
        FileBuffer instance = null;
        long expResult = 0L;
        long result = instance.read(count);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of write method, of class FileBuffer.
     */
    @Test @Ignore
    public void testWrite() throws Exception {
        System.out.println("write");
        FileChannel channel = new FileOutputStream(folder.newFile()).getChannel();
        ByteBuffer  direct = ByteBuffer.allocateDirect(10 * 1024);
        int count = 0;
        FileBuffer instance = new FileBuffer(channel, direct);
        long expResult = 0L;
        long result = instance.write(count);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    ByteBuffer[] produceArray(int...sizes) {
        ArrayList<ByteBuffer> bufs = new ArrayList<ByteBuffer>();
        for ( int i : sizes ) {
            bufs.add(ByteBuffer.allocate(i));
        }
        return bufs.toArray(new ByteBuffer[bufs.size()]);
    }


    /**
     * Test of insert method, of class FileBuffer.
     */
    @Test @Ignore
    public void testInsert() throws Exception {
        System.out.println("insert");
        ByteBuffer[] bufs = null;
        int loc = 0;
        FileBuffer instance = null;
        instance.insert(bufs, loc, false);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class FileBuffer.
     */
    @Test @Ignore
    public void testClose() throws Exception {
        System.out.println("close");
        FileBuffer instance = null;
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
    
    @Test
    public void overlapBufferMove() throws Exception {
        FileChannel channel = mock(FileChannel.class);
        FileBuffer buffer = new FileBuffer(channel, ByteBuffer.allocate(100 * 1024));
        byte[] fill = new byte[128];
        for (int x = 0; x < fill.length; x++) {
            fill[x] = RandomGenerator.lessThan(Byte.MAX_VALUE);
        }
        buffer.put(fill);
        buffer.bufferMove(0, 64, 128);
        for (int x = 0; x < fill.length; x++) {
            assert (buffer.get(x + 64) == fill[x]);
        }
    }
}
