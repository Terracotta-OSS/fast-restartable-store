/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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
  
  @Rule
  public JUnitTestFolder folder = new JUnitTestFolder();
  
  
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
    home = spy(folder.newFolder());
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
    verify(home).getUsableSpace();
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
    verify(stream).findLogTail();
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
