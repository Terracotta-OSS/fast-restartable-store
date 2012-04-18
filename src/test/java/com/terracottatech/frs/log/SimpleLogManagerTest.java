/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.Future;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author mscott
 */
public class SimpleLogManagerTest {
  private static final long LOG_REGION_WRITE_TIMEOUT = 10;

  private IOManager  ioManager;
  private LogManager logManager;

  @Before
  public void setUp() throws Exception {
    ioManager = mock(IOManager.class);
    logManager = new SimpleLogManager(ioManager);
  }

  /**
   * Test of startup method, of class SimpleLogManager.
   */
  @Test
  @Ignore
  public void testStartup() {
    SimpleLogManager instance = null;
    instance.startup();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of shutdown method, of class SimpleLogManager.
   */
  @Test
  @Ignore
  public void testShutdown() {
    SimpleLogManager instance = null;
    instance.shutdown();
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of totalBytes method, of class SimpleLogManager.
   */
  @Test
  @Ignore
  public void testTotalBytes() {
    SimpleLogManager instance = null;
    long expResult = 0L;
    long result = instance.totalBytes();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  /**
   * Test of appendAndSync method, of class SimpleLogManager.
   */
  @Test
  public void testAppendAndSync() throws Exception {
    logManager.startup();
    LogRecord record = newRecord(100, -1);
    Future<Void> f = logManager.appendAndSync(record);
    f.get(LOG_REGION_WRITE_TIMEOUT, SECONDS);
    verify(ioManager).write(any(Chunk.class));
  }

  /**
   * Test of append method, of class SimpleLogManager.
   */
  @Test
  public void testAppend() throws Exception {
    logManager.startup();
    Future<Void> writeLsn = null;
    for (long i = 100; i < 200; i++) {
      LogRecord record = newRecord(i, -1);
      writeLsn = logManager.append(record);
      verify(record).updateLsn(i);
    }
    writeLsn.get(LOG_REGION_WRITE_TIMEOUT, SECONDS);
    verify(ioManager).write(any(Chunk.class));
  }

  /**
   * Test of reader method, of class SimpleLogManager.
   */
  @Test
  @Ignore
  public void testReader() {
    SimpleLogManager instance = null;
    Iterator expResult = null;
    Iterator result = instance.reader();
    assertEquals(expResult, result);
    // TODO review the generated test code and remove the default call to fail.
    fail("The test case is a prototype.");
  }

  private LogRecord newRecord(long lsn, long lowest) {
    LogRecord r = mock(LogRecord.class);
    doReturn(lsn).when(r).getLsn();
    doReturn(lowest).when(r).getLowestLsn();
    doReturn(new ByteBuffer[0]).when(r).getPayload();
    return r;
  }
}
