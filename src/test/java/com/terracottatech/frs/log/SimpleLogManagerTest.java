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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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
    LogRecord record = newRecord(100);
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
      LogRecord record = spy(newRecord(-1));
      writeLsn = logManager.append(record);
      verify(record).updateLsn(i);
    }
    writeLsn.get(LOG_REGION_WRITE_TIMEOUT, SECONDS);
    verify(ioManager).write(any(Chunk.class));
  }

  @Test
  public void testMultiThreadedAppendAndSync() throws Exception {
    logManager.startup();
    ExecutorService executorService = Executors.newFixedThreadPool(20);
    List<Callable<Void>> runnables = new ArrayList<Callable<Void>>();
    Random r = new Random();
    final AtomicInteger syncs = new AtomicInteger();
    for (int i = 0; i < 10000; i++) {
      if (r.nextInt(100) < 25) {
        runnables.add(new Callable<Void>() {
          @Override
          public Void call() {
            logManager.appendAndSync(newRecord(-1));
            syncs.incrementAndGet();
            return null;
          }
        });
      } else {
        runnables.add(new Callable<Void>() {
          @Override
          public Void call() {
            logManager.append(newRecord(-1));
            return null;
          }
        });
      }
    }
    for (Future<Void> future : executorService.invokeAll(runnables)) {
      future.get();
    }
    executorService.shutdown();

    // Some of the syncs can wind up overlapping, so let's say at least 50% of them
    // can trigger a new write.
    verify(ioManager, atLeast(syncs.get() / 2)).write(any(Chunk.class));
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

  private LogRecord newRecord(long lowest) {
    return new LogRecordImpl(lowest, new ByteBuffer[0], mock(LSNEventListener.class));
  }
}
