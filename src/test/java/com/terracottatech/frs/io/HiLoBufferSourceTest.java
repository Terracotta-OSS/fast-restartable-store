/*
 * All content copyright (c) 2014 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */

package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
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
public class HiLoBufferSourceTest extends BufferSourceTest {
  

  public HiLoBufferSourceTest() {
  }

  @Override
  public BufferSource getBufferSource() {
    return new HiLoBufferSource(1024, 8*1024*1024, 64*1024*1024);
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  @Override
  public void setUp() {
    super.setUp();
  }
  
  @After
  public void tearDown() {
  }
  
}
