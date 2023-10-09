/*
 * Copyright (c) 2014-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.ArgumentMatchers;
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
    when(parent.getBuffer(ArgumentMatchers.anyInt())).then(new Answer<ByteBuffer> () {

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
