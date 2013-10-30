/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
/**
 *
 * @author mscott
 */
public class MaskingBufferSourceTest {
  
  public BufferSource parent;
  public MaskingBufferSource src;
  
  public MaskingBufferSourceTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @Before 
  public void setup() {
    parent = mock(BufferSource.class);
    when(parent.getBuffer(Matchers.anyInt())).then(new Answer<ByteBuffer> () {

      @Override
      public ByteBuffer answer(InvocationOnMock invocation) throws Throwable {
        return ByteBuffer.allocate((Integer)invocation.getArguments()[0]);
      }
      
    });
    src = new MaskingBufferSource(parent);
  }

  /**
   * Test of getBuffer method, of class MaskingBufferSource.
   */
  @Test
  public void testGetBuffer() {
    System.out.println("getBuffer");
    int size = 1023;
    ByteBuffer answer = src.getBuffer(size);
    verify(parent).getBuffer(Matchers.anyInt());
    assertEquals(answer.capacity(), size);
  }

  /**
   * Test of returnBuffer method, of class MaskingBufferSource.
   */
  @Test
  public void testReturnBuffer() {
    System.out.println("returnBuffer");
    int size = 1023;
    ByteBuffer answer = src.getBuffer(size);
    verify(parent).getBuffer(Matchers.anyInt());
    src.returnBuffer(answer);
    ArgumentCaptor<ByteBuffer> capture = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(parent).returnBuffer(capture.capture());
    assertEquals(capture.getValue().capacity(), 1023);
  }

  /**
   * Test of reclaim method, of class MaskingBufferSource.
   */
  @Test
  public void testReclaim() {
    System.out.println("returnBuffer");
    int size = 1023;
    ByteBuffer answer = src.getBuffer(size);
    verify(parent).getBuffer(Matchers.anyInt());
    src.reclaim();
    ArgumentCaptor<ByteBuffer> capture = ArgumentCaptor.forClass(ByteBuffer.class);
    verify(parent).returnBuffer(capture.capture());
    assertEquals(capture.getValue().capacity(), 1023);
  }

}
