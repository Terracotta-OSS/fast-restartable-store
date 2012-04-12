/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.MasterLogRecordFactory;
import com.terracottatech.frs.log.Signature;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.junit.*;
import static org.junit.Assert.*;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author mscott
 */
public class NIOStreamImplTest {

    NIOStreamImpl stream;
    File workarea;
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    

    public NIOStreamImplTest() {
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
            workarea = folder.newFolder();
            stream = new NIOStreamImpl(workarea.getAbsolutePath(), (10 * 1024 * 1024));
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
    }

    @After
    public void tearDown() {
        try {
            stream.close();
        } catch (IOException ioe) {
            throw new AssertionError(ioe);
        }
    }

    /**
     * Test of shutdown method, of class NIOStreamImpl.
     */
    @Test
    public void testShutdown() {
        System.out.println("shutdown");
        stream.shutdown();
        assertFalse(new File(workarea.getAbsolutePath() + "/FRS.lck").exists());
    }

    /**
     * Test of append method, of class NIOStreamImpl.
     */
    @Test
    public void testAppend() throws Exception {
        System.out.println("append");
        Segment s = stream.append();
        assertNotNull(s);
        assertThat(s,instanceOf(Segment.class));
        assertFalse(s.isClosed());
    }

    /**
     * Test of sync method, of class NIOStreamImpl.
     */
    @Test
    public void testSync() throws Exception {
        System.out.println("sync");
        Segment s = stream.append();
        s.append(new LogRegionPacker(Signature.ADLER32).pack(new TestLogRegion(Arrays.asList(new LogRecord[] {new TestLogRecord()}))));
        stream.sync();
        File lock = new File(workarea.getAbsolutePath() + "/FRS.lck");
        assertTrue(lock.exists());
        FileChunk chunk = new FileChunk(new File(workarea.getAbsolutePath() + "/FRS.lck"),ByteBuffer.allocate((int)lock.length()));
        System.out.format("segment: %d position: %d",chunk.getInt(),chunk.getLong());
    }

    /**
     * Test of close method, of class NIOStreamImpl.
     */
    @Test
    public void testClose() throws Exception {
        System.out.println("close");
        stream.close();
        assertFalse(new File(workarea.getAbsolutePath() + "/FRS.lck").exists());
    }

    /**
     * Test of iterator method, of class NIOStreamImpl.
     */
    @Test @Ignore
    public void testIterator() {
        System.out.println("iterator");
    }
}
