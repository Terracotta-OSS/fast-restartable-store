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

import com.terracottatech.frs.util.JUnitTestFolder;
import java.io.File;
import java.util.Random;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Rule;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 * @author mscott
 */
public class LiveNIOStatisticsTest {
  
  LiveNIOStatistics stats;
  NIOStreamImpl    stream;
  File             home;
  final long             written;
  final long             read;
  
  
  public LiveNIOStatisticsTest() {
    Random r = new Random();
    written = r.nextLong();
    read = r.nextLong();
  }
  
  @BeforeClass
  public static void setUpClass() {
  }
  
  @AfterClass
  public static void tearDownClass() {
  }
  
  @Before
  public void setUp() throws Exception {    
    home = mock(File.class);
    when(home.getUsableSpace()).thenReturn(Long.MAX_VALUE);
    stream = mock(NIOStreamImpl.class);
    stats = new LiveNIOStatistics(home, stream, written, read);
  }
  
  @After
  public void tearDown() {
  }

  /**
   * Test of getTotalAvailable method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetTotalAvailable() {
    System.out.println(stats.getTotalAvailable());
    stream.getMarker();
    verify(home).getUsableSpace();
    verify(stream).getMarker();
    
  }

  /**
   * Test of getTotalUsed method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetTotalUsed() {
    System.out.println(stats.getTotalUsed());
    verify(stream).getTotalSize();
    
  }

  /**
   * Test of getTotalWritten method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetTotalWritten() throws Exception {
    assertEquals(written, stats.getTotalWritten());
  }

  /**
   * Test of getTotalRead method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetTotalRead() {
    assertEquals(read, stats.getTotalRead());
  }

  /**
   * Test of getLiveSize method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetLiveSize() throws Exception {
    System.out.println(stats.getLiveSize());
    verify(stream).scanForEnd();
    verify(stream).getTotalSize();
  }

  /**
   * Test of getExpiredSize method, of class LiveNIOStatistics.
   */
  @Test
  public void testGetExpiredSize() throws Exception {
    System.out.println(stats.getExpiredSize());
    verify(stream).getTotalSize();
    verify(stream).findLogTail();
  }
  
}
