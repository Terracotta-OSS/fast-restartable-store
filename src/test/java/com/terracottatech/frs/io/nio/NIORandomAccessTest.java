/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.HeapBufferSource;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author mscott
 */
public class NIORandomAccessTest {
    
  @Rule
  public JUnitTestFolder tempFolder = new JUnitTestFolder();
  public File baseDir;
  NIOStreamImpl stream;
  NIORandomAccess ra;
    
    public NIORandomAccessTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() throws Throwable {
        baseDir = tempFolder.newFolder();
//        stream = new MockNIOStream();
        stream = Mockito.mock(NIOStreamImpl.class);
        Mockito.when(stream.getAccessMethod()).thenReturn(NIOAccessMethod.STREAM);
        Mockito.when(stream.getStreamId()).thenReturn(UUID.randomUUID());
        Mockito.when(stream.createFileBuffer(Matchers.any(FileChannel.class), Matchers.anyInt())).thenAnswer(new Answer<FileBuffer>() {

            @Override
            public FileBuffer answer(InvocationOnMock invocation) throws Throwable {
                return new FileBuffer((FileChannel)invocation.getArguments()[0],ByteBuffer.allocate(1024));
            }
            
        });
                
        NIOSegmentList segments = Mockito.mock(NIOSegmentList.class);
        mockFiles(segments);
        ra = new NIORandomAccess(stream, segments, new HeapBufferSource(512*1024*1024));
    }
    
    private void mockFiles(NIOSegmentList list) {
        StringBuilder fn = new StringBuilder();
        Formatter pfn = new Formatter(fn);

        for (int x=0;x<2;x++) {
            pfn.format(NIOConstants.SEGMENT_NAME_FORMAT, x);
            File f = new File(baseDir,fn.toString());
            insertTestData(stream, f, x);
            Mockito.when(list.getFile(x)).thenReturn(f);
            fn.setLength(0);
        }
    }
    
    private static void insertTestData(NIOStreamImpl stream, File f, int segno) {
        try {
            WritingSegment seg = new WritingSegment(stream,f).open();
            seg.insertFileHeader(100, 100 + segno);
            seg.append(new WrappingChunk(ByteBuffer.wrap(("MAGIC" + segno).getBytes())), 100 + segno + 1);
            seg.prepareForClose();
            seg.close();
        } catch ( Throwable t ) {
            throw new RuntimeException(t);
        }
    }
        
    @After
    public void tearDown() {
    }

    /**
     * Test of scan method, of class NIORandomAccess.
     */
    @Test 
    public void testScan() throws Exception {
        System.out.println("scan");
        NIORandomAccess instance = ra;
        byte[] result = new byte[6];
        instance.scan(101L).get(result);
        System.out.println(new String(result));
        assertEquals("MAGIC0", new String(result));
//  test hop to second file
        instance.scan(102L).get(result);
        System.out.println(new String(result));
        assertEquals("MAGIC1", new String(result));
    }

    /**
     * Test of seek method, of class NIORandomAccess.
     */
    @Test
    public void testSeek() throws Exception {
        System.out.println("seek");
        NIORandomAccess instance = ra;
        ReadOnlySegment result = instance.seek(102L);
        assertEquals(result.getSegmentId(), 1);
    }

    /**
     * Test of hint method, of class NIORandomAccess.
     */
    @Test @Ignore
    public void testHint() throws Throwable {
        System.out.println("hint");
        long marker = 0L;
        int segment = 0;
        NIORandomAccess instance = ra;
// change index to next file
        instance.hint(101L, 1);

        assertEquals(instance.seek(101L).getSegmentId(),1);
    }
    
    private class MockNIOStream extends NIOStreamImpl {

        public MockNIOStream() throws IOException {
            super(baseDir,1024 * 1024);
        }
        
    }
    
    private class MockNIOSegmentList extends NIOSegmentList {
        
        Map<Integer,File> mockFiles = new TreeMap<Integer, File>();

        public MockNIOSegmentList() throws IOException {
            super(baseDir);
        }

        @Override
        synchronized File getFile(int segmentId) {
            File f = mockFiles.get(segmentId);
            if ( f == null ) {
                StringBuilder fn = new StringBuilder();
                Formatter pfn = new Formatter(fn);

                pfn.format(NIOConstants.SEGMENT_NAME_FORMAT, segmentId);

                f = new File(baseDir,fn.toString());
                mockFiles.put(segmentId,f);
                insertTestData(stream, f,segmentId);
            }
            return f;
        }     
    }
}