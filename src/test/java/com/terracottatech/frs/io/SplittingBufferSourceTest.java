/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
