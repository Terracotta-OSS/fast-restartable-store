/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.rules.TemporaryFolder;
import static org.mockito.Mockito.mock;

/**
 *
 * @author mscott
 */
public class NIOSegmentImplTest {

    NIOSegmentImpl tester;
    
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
    public void setUp() {
        try {
            tester = new NIOSegmentImpl(folder.newFile(), (10 * 1024 * 1024), false);
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
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
        Chunk c = new Chunk() {

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
            assertEquals(40l, result);
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
        long expResult = 0L;
        long result = tester.length();
        assertEquals(expResult, result);
    }

    /**
     * Test of remains method, of class NIOSegmentImpl.
     */
    @Test
    public void testRemains() throws Exception {
        System.out.println("remains");
        long expResult = (10L*1024*1024);
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
