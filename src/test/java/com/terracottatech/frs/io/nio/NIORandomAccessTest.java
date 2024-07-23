/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.HeapBufferSource;
import com.terracottatech.frs.io.WrappingChunk;
import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;
import java.util.UUID;

import net.bytebuddy.implementation.bytecode.Addition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.mockito.AdditionalMatchers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;

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
        Mockito.when(stream.createFileBuffer(ArgumentMatchers.any(FileChannel.class), ArgumentMatchers.anyInt())).thenAnswer(new Answer<FileBuffer>() {

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
            seg.insertFileHeader(Constants.FIRST_LSN, 100 + segno);
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
    
    @Test
    public void testScanTooFast() throws Exception {
      final NIORandomAccess.FileCache mocked = Mockito.mock(NIORandomAccess.FileCache.class);
      final ReadOnlySegment seg = Mockito.mock(ReadOnlySegment.class);
      Mockito.when(mocked.findSegment(ArgumentMatchers.anyInt())).thenReturn(null);
      Mockito.when(mocked.removeSegments(ArgumentMatchers.anyInt())).thenReturn(mocked);
      Mockito.when(mocked.addSegment(ArgumentMatchers.any(ReadOnlySegment.class))).then(new Answer<Object> () {

        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          return mocked;
        }
      });
      
      Mockito.when(seg.scan(AdditionalMatchers.geq(500L))).thenReturn(new WrappingChunk(ByteBuffer.wrap(new byte[0])));
      Mockito.when(seg.load(ArgumentMatchers.any(BufferSource.class))).thenReturn(seg);
      Mockito.when(seg.getBaseMarker()).thenReturn(500L);

      ra.seedCache(mocked);
      Chunk c = ra.scan(501);
      assertNull(c);
//  mock lsn now committed to disk
      Mockito.when(mocked.findSegment(AdditionalMatchers.geq(2))).then(new Answer<Object> () {

        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          int sid = (Integer)invocation.getArguments()[0];
          Mockito.when(seg.getSegmentId()).thenReturn(sid);
          return seg;
        }
      });
      c = ra.scan(501);
      assertNotNull(c);
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
}
