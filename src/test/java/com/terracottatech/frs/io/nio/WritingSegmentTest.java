package com.terracottatech.frs.io.nio;

/*
 * All content copyright (c) 2014 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

import com.terracottatech.frs.io.FileBuffer;
import com.terracottatech.frs.io.WrappingChunk;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
/**
 *
 * @author mscott
 */
public class WritingSegmentTest extends NIOSegmentTest {
  
  private NIOStreamImpl stream;
  private WritingSegment writer;
  
  public WritingSegmentTest() {
  }
  
  @Before
  @Override
  public void setUp() throws IOException {
    super.setUp();
    stream = mock(NIOStreamImpl.class);
    when(stream.getStreamId()).thenReturn(UUID.randomUUID());
    try {
      when(stream.createFileBuffer(any(FileChannel.class), anyInt())).thenAnswer(new Answer() {
        public FileBuffer answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          Object mock = invocation.getMock();
          try {
            return new FileBuffer((FileChannel)args[0],ByteBuffer.allocate((Integer)args[1]));
          } catch ( IOException ioe ) {
            throw new RuntimeException(ioe);
          }
        }
      });
    } catch ( IOException ioe ) {
      throw new RuntimeException(ioe);
    }

  }
  
  @After
  @Override
  public void tearDown() {
    super.tearDown();
  }

   @Test
  public void testNewOpen() throws Exception {
    writer = new WritingSegment(stream, new File(getDirectory(),"seg001.frs"));
    Assert.assertFalse(writer.open().isClosed());
  }
  
   @Test
  public void testExistingOpen() throws Exception {
    writer = new WritingSegment(stream, new File(getDirectory(),"seg002.frs"));
    writer.open();
    writer.insertFileHeader(1000, 1100);
    writer.prepareForClose();
    writer.close();
    writer = new WritingSegment(stream, new File(getDirectory(),"seg002.frs"));
    Assert.assertFalse(writer.open().isClosed());
  }
  
   @Test
  public void testCleanClose() throws Exception {
    writer = new WritingSegment(stream, new File(getDirectory(),"seg003.frs"));
    writer.open();
    writer.insertFileHeader(1000, 1100);
    writer.append(new WrappingChunk(ByteBuffer.allocate(256)), 1101);
    
    writer.prepareForClose();
    writer.close();
    Assert.assertTrue(new NIOSegment(stream, new File(getDirectory(),"seg003.frs")).openForHeader().wasProperlyClosed());
  }
  
   @Test
  public void testAbruptClose() throws Exception {
    writer = new WritingSegment(stream, new File(getDirectory(),"seg004.frs"));
    writer.open();
    writer.insertFileHeader(1000, 1100);
    writer.append(new WrappingChunk(ByteBuffer.allocate(256)), 1101);
    writer.close();
    Assert.assertFalse(new NIOSegment(stream, new File(getDirectory(),"seg004.frs")).openForHeader().wasProperlyClosed());
  }

  @Test
  public void testCloseWithoutOpen() throws Exception {  // can happen when open errors
    writer = new WritingSegment(stream, new File(getDirectory(),"seg005.frs"));
    writer.prepareForClose();
    writer.close();
  }

}