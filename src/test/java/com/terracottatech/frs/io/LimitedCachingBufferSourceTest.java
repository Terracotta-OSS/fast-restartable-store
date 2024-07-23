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
package com.terracottatech.frs.io;

import java.nio.ByteBuffer;
import org.hamcrest.number.OrderingComparison;
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
public class LimitedCachingBufferSourceTest {
  
  
  public LimitedCachingBufferSourceTest() {
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() {
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of returnBuffer method, of class LimitedCachingBufferSource.
   */
  @Test
  public void testReturnBuffer() {
    LimitedCachingBufferSource src = new LimitedCachingBufferSource(1024*1024);
    System.out.println("returnBuffer");
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    src.returnBuffer(buffer);
    assertEquals("returned buffer is retained", src.getSize(), buffer.capacity());
  }

  /**
   * Test of getBuffer method, of class LimitedCachingBufferSource.
   */
  @Test
  public void testGetBuffer() {
    LimitedCachingBufferSource src = new LimitedCachingBufferSource(1024*1024);
    System.out.println("getBuffer");
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    src.returnBuffer(buffer);
    buffer = src.getBuffer(512);
    assertNotNull(buffer);
    buffer = src.getBuffer(512);
    assertNull(buffer);
  }
  
  @Test
  public void testBufferSplit() {
    LimitedCachingBufferSource src = new LimitedCachingBufferSource(1024*1024);
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);
    src.returnBuffer(buffer);
    buffer = src.getBuffer(256);
    assertNotNull(buffer);
    buffer = src.getBuffer(256);
    assertNotNull(buffer);
    buffer = src.getBuffer(512);
    assertNull(buffer);
  }  

  /**
   * Test of getLimit method, of class LimitedCachingBufferSource.
   */
  @Test
  public void testGetLimit() {
    LimitedCachingBufferSource src = new LimitedCachingBufferSource(1024*1024);
    assertEquals(src.getLimit(), 1024 * 1024);
  }
  
  @Test
  public void testLimiting() {
    LimitedCachingBufferSource src = new LimitedCachingBufferSource(1024*1024);
    
    for (int x=0;x<2048;x++) {
      src.returnBuffer(ByteBuffer.allocate(1024));
    }
    assertThat(src.getSize(), OrderingComparison.lessThanOrEqualTo(src.getLimit()));
  }  
  
}
