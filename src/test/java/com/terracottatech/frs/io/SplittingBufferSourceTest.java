/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mscott
 */
public class SplittingBufferSourceTest {
  
  public SplittingBufferSource src;
  public static final int MIN = 64;
  public static final int MAX = 1024 * 1024;
  
  public SplittingBufferSourceTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
    src = new SplittingBufferSource(MIN,MAX);
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of getBuffer method, of class SplittingBufferSource.
   */
  @Test
  public void testGetBuffer() {
    System.out.println("getBuffer");
    int count = 0;
    
    ByteBuffer b = src.getBuffer(50);
    while ( b != null ) {
      count++;
      b = src.getBuffer(50);
    }
    assertTrue(count * MIN >= MAX);
  }

  /**
   * Test of returnBuffer method, of class SplittingBufferSource.
   */
  @Test
  public void testReturnBuffer() {
    System.out.println("returnBuffer");
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    int expected = src.available();
    ByteBuffer b = src.getBuffer(50);
    while ( b != null ) {
      list.add(b);
      b = src.getBuffer(50);
    }
    for ( ByteBuffer cc : list ) {
      src.returnBuffer(cc);
    }
    assertEquals(expected, src.available());
  }

  /**
   * Test of reclaim method, of class SplittingBufferSource.
   */
  @Test
  public void testReclaim() {
    System.out.println("reclaim");
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    ByteBuffer b = src.getBuffer(50);
    while ( b != null ) {
      list.add(b);
      b = src.getBuffer(50);
    }
    for ( ByteBuffer cc : list ) {
      src.returnBuffer(cc);
    }
    assertEquals(MIN, src.largestChunk());
    src.reclaim();
    assertEquals(MAX, src.largestChunk());
  }
  
}
