/*
 * All content copyright (c) 2014 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Matchers;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 *
 * @author mscott
 */
public abstract class BufferSourceTest {
  
  public BufferSource parent;
  public BufferSource src;

  public BufferSourceTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  public abstract BufferSource getBufferSource();
  
  @Before
  public void setUp() {
     parent = mock(BufferSource.class);
    when(parent.getBuffer(Matchers.anyInt())).then(new Answer<ByteBuffer> () {

      @Override
      public ByteBuffer answer(InvocationOnMock invocation) throws Throwable {
        return ByteBuffer.allocate((Integer)invocation.getArguments()[0]);
      }
      
    });
    src = getBufferSource();
  }

  /**
   * Test of getBuffer method, of class MaskingBufferSource.
   */
  @Test
  public void testGetBuffer() {
    System.out.println("getBuffer");
    int size = 1023;
    ByteBuffer answer = src.getBuffer(size);
    assertEquals(answer.remaining(), size);
  }

  /**
   * Test of returnBuffer method, of class MaskingBufferSource.
   */
  @Test
  public void testReturnBuffer() {

  }

  /**
   * Test of reclaim method, of class MaskingBufferSource.
   */
  @Test
  public void testReclaim() {

  }

  
}
