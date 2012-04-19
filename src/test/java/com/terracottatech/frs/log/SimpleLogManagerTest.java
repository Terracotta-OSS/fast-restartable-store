/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Direction;
import com.terracottatech.frs.io.IOManager;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;

/**
 * @author mscott
 */
public class SimpleLogManagerTest {
  private static final long LOG_REGION_WRITE_TIMEOUT = 10;

  private DummyIOManager ioManager;
  private LogManager logManager;

  @Before
  public void setUp() throws Exception {
    ioManager = spy(new DummyIOManager());
    logManager = new SimpleLogManager(ioManager);
  }

  /**
   * Test of appendAndSync method, of class SimpleLogManager.
   */
  @Test
  public void testAppendAndSync() throws Exception {
    logManager.startup();
    LogRecord record = newRecord(-1);
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
    for (long i = 100; i < 200; i++) {
      LogRecord record = spy(newRecord(-1));
      logManager.append(record);
      verify(record).updateLsn(i);
    }
    logManager.shutdown();
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
  public void testReader() throws Exception {
    LogRegionPacker packer = new LogRegionPacker(Signature.ADLER32);
    long lsn = 100;
    for (int i = 0; i < 10; i++) {
      List<LogRecord> records = new ArrayList<LogRecord>();
      for (int j = 0; j < 100; j++) {
        LogRecord record = newRecord(-1);
        record.updateLsn(lsn);
        lsn++;
        records.add(record);
      }
      ioManager.write(packer.pack(records));
    }
    logManager.startup();

    long expectedLsn = 1099;
    Iterator<LogRecord> i = logManager.reader();
    while (i.hasNext()) {
      LogRecord record = i.next();
      assertThat(record.getLowestLsn(), is(-1L));
      assertThat(record.getLsn(), is(expectedLsn));
      expectedLsn--;
    }
    assertThat(expectedLsn, is(99L));
  }

  private LogRecord newRecord(long lowest) {
    return new LogRecordImpl(lowest, new ByteBuffer[0], mock(LSNEventListener.class));
  }

  private class DummyIOManager implements IOManager {
    private final Deque<Chunk> chunks = new LinkedList<Chunk>();

    @Override
    public long write(Chunk region) throws IOException {
      chunks.push(region);
      return 0;
    }

    @Override
    public void setLowestLsn(long lsn) throws IOException {
    }

    @Override
    public Chunk read(Direction dir) throws IOException {
      if (chunks.isEmpty()) return null;
      return chunks.pop();
    }

    @Override
    public long seek(long lsn) throws IOException {
      return 0;
    }

    @Override
    public void sync() throws IOException {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
