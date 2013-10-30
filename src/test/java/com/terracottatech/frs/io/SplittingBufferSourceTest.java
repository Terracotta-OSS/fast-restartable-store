/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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
    src = new SplittingBufferSource(64,1024 * 1024);
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
    assertTrue(count * 64 >= 1024 * 1024);
  }

  /**
   * Test of returnBuffer method, of class SplittingBufferSource.
   */
  @Test
  public void testReturnBuffer() {
    System.out.println("returnBuffer");
    List<ByteBuffer> list = new ArrayList<ByteBuffer>();
    ByteBuffer b = src.getBuffer(50);
    while ( b != null ) {
      list.add(b);
      b = src.getBuffer(50);
    }
    for ( ByteBuffer cc : list ) {
      src.returnBuffer(cc);
    }
    assertEquals(1024 * 1024, src.available());
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
    assertEquals(64, src.largestChunk());
    src.reclaim();
    assertEquals(1024 * 1024, src.largestChunk());
  }
  
}
