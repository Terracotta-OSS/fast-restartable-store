/*
 * Copyright (c) 2017-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class WrappedFileChannelTest {
  private static final int BUF_LEN = 10 * 1024;
  private static final byte[] SRC_BUF = new byte[BUF_LEN];

  static {
    for (int i = 0; i < BUF_LEN; i++) {
      SRC_BUF[i] = (byte) (i ^ (i >> 24));
    }
  }

  private static final int BIG_BUF_LEN = 10 * 1024 * 1024;
  private static final byte[] BIG_SRC_BUF = new byte[BIG_BUF_LEN];

  static {
    ByteBuffer b = ByteBuffer.wrap(BIG_SRC_BUF);
    for (int i = 0; i < BIG_BUF_LEN/Integer.BYTES; i++) {
      b.putInt(i);
    }
  }

  private static final ByteBuffer SRC = ByteBuffer.wrap(SRC_BUF);
  private static final ByteBuffer BIG_SRC = ByteBuffer.wrap(BIG_SRC_BUF);

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  private WrappedFileChannel fileChannelForWrite;
  private WrappedFileChannel fileChannelForRead;
  private TestWriteChannelOpener writeOpener;
  private TestReadChannelOpener readOpener;
  private ExecutorService threadPool;
  private List<Thread> runningThreads;

  @Before
  public void setUp() throws Exception {
    File fileUnderTest = tempFolder.newFile();
    readOpener = new TestReadChannelOpener(fileUnderTest);
    writeOpener = new TestWriteChannelOpener(fileUnderTest);
    fileChannelForRead = new WrappedFileChannel(readOpener.open(), readOpener);
    fileChannelForWrite = new WrappedFileChannel(writeOpener.open(), writeOpener);
    threadPool = Executors.newFixedThreadPool(10);
    runningThreads = Collections.synchronizedList(new LinkedList<Thread>());
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
    readOpener.close();
    writeOpener.close();
  }

  @Test
  public void testReadWrite() throws Exception {
    writeBuf(0L, true);
    ByteBuffer dst = readBuf(0L, true);
    assertThat(BIG_SRC.array().length, is(BIG_BUF_LEN));
    assertThat(dst.array().length, is(BIG_BUF_LEN));
    assertArrayEquals(BIG_SRC.array(), dst.array());
  }

  @Test
  public void testReadWriteWithPos() throws Exception {
    writeBuf(0L, false);
    writeBuf(BUF_LEN * 2, false);
    ByteBuffer dst = readBuf(0L, false);
    assertArrayEquals(SRC.array(), dst.array());
    dst = readBuf(BUF_LEN * 2, false);
    assertArrayEquals(SRC.array(), dst.array());
  }

  @Test
  public void testSize() throws Exception {
    writeBuf(0L, false);
    writeBuf(BUF_LEN * 2, false);
    fileChannelForWrite.force(true);
    long expectedSize = (BUF_LEN * 2) + BUF_LEN;
    assertThat(fileChannelForWrite.size(), is(expectedSize));
    assertThat(fileChannelForRead.size(), is(expectedSize));
  }

  @Test
  public void testInterruptedWrite() throws Exception {
    testLoop(threadPool, false, false, writeLoopRunner(0L));
    assertThat(writeOpener.getReopenHappenedCount(), greaterThanOrEqualTo(1));
  }

  @Test
  public void testInterruptedWriteNoPos() throws Exception {
    testLoop(threadPool, false, false, writeLoopRunner(-1L));
    assertThat(writeOpener.getReopenHappenedCount(), greaterThanOrEqualTo(1));
    fileChannelForRead.force(true);
    long size = fileChannelForRead.size();
    ByteBuffer dst = readBuf(size - BUF_LEN, false);
    assertArrayEquals(SRC.array(), dst.array());
  }

  @Test
  public void testInterruptedRead() throws Exception {
    writeBuf(0L, false);
    testLoop(threadPool, false, false, readLoopRunner(0L));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(1));
  }

  @Test
  public void testInterruptedWrites() throws Exception {
    testLoop(threadPool, false, false,
        writeLoopRunner(0L),
        writeLoopRunner(BUF_LEN * 2),
        writeLoopRunner(BUF_LEN * 4));
    assertThat(writeOpener.getReopenHappenedCount(), greaterThanOrEqualTo(3));
  }

  @Test
  public void testInterruptedReads() throws Exception {
    long[] positions = {0L, 2 * BUF_LEN, 8 * BUF_LEN};
    for (long position : positions) {
      writeBuf(position, false);
    }
    testLoop(threadPool, false, false,
        readLoopRunner(positions[0]),
        readLoopRunner(positions[1]),
        readLoopRunner(positions[2]));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(3));
  }

  @Test
  public void testInterruptedReadsWithLock() throws Exception {
    long[] positions = {0L, 32L * BUF_LEN, 64L * BUF_LEN};
    for (long position : positions) {
      writeBuf(position, false);
      fileChannelForRead.lock(position, BUF_LEN, true);
    }
    testLoop(threadPool, false, false,
        readLoopRunner(positions[0]),
        readLoopRunner(positions[1]),
        readLoopRunner(positions[2]));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(3));
    try {
      fileChannelForRead.tryLock(positions[0], BUF_LEN, true);
      fail("Lock is not held post interrupt");
    } catch (OverlappingFileLockException ignored) {
    }
  }

  @Test
  public void testInterruptedWritesWithLock() throws Exception {
    long[] positions = {0L, 8 * BUF_LEN, 16 * BUF_LEN, 32 * BUF_LEN, 64 * BUF_LEN};
    for (long position : positions) {
      if (position >= 32 * BUF_LEN) {
        fileChannelForWrite.lock(position, BUF_LEN, false);
      } else {
        fileChannelForWrite.tryLock(position, BUF_LEN, false);
      }
    }
    testLoop(threadPool, true, false,
        writeLoopRunner(positions[0], false),
        writeLoopRunner(positions[1], false),
        writeLoopRunner(positions[2], false),
        writeLoopRunner(positions[3], false),
        writeLoopRunner(positions[4], false));
    try {
      fileChannelForWrite.tryLock(positions[4], BUF_LEN, false);
      fail("Lock is not held post interrupt");
    } catch (OverlappingFileLockException ignored) {
    }
    assertThat(writeOpener.getReopenHappenedCount(), greaterThanOrEqualTo(5));
  }

  @Test
  public void testInterruptedScatteredRead() throws Exception {
    writeBuf(0L, true);
    testLoop(threadPool, true, true,
        scatteredReadLoopRunner(0));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(0));
  }

  @Test
  public void testInterruptedTryLocks() throws Exception {
    long[] positions = {0L, 8 * BUF_LEN, 16 * BUF_LEN};
    for (long position : positions) {
      writeBuf(position, false);
    }
    testLoop(threadPool, true, false,
        lockLoopRunner(positions[0], true),
        lockLoopRunner(positions[1], true),
        lockLoopRunner(positions[2], true));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(3));
  }

  @Test
  public void testInterruptedLocks() throws Exception {
    long[] positions = {0L, 8 * BUF_LEN, 16 * BUF_LEN};
    for (long position : positions) {
      writeBuf(position, false);
    }
    testLoop(threadPool, true, false,
        lockLoopRunner(positions[0], false),
        lockLoopRunner(positions[1], false),
        lockLoopRunner(positions[2], false));
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(3));
  }

  @Test
  public void testInterruptedMapAndSize() throws Exception {
    writeBuf(0, true);
    testLoop(threadPool, true, true, mapAndSizeLoopRunner());
    assertThat(readOpener.getReopenHappenedCount(), greaterThanOrEqualTo(1));
  }

  private void testLoop(ExecutorService executor, boolean writeLocked, boolean big,
                        final Runnable... runners) throws Exception {
    final int numRunners = runners.length;
    final CountDownLatch beginLatch = new CountDownLatch(numRunners);
    final CountDownLatch endLatch = new CountDownLatch(numRunners);
    List<Future> futures = new LinkedList<>();
    for (final Runnable r : runners) {
      futures.add(executor.submit(() -> {
        runningThreads.add(Thread.currentThread());
        beginLatch.countDown();
        try {
          r.run();
        } catch (Throwable e) {
          System.out.println("Unexpected Exception ");
          e.printStackTrace();
        } finally {
          endLatch.countDown();
          runningThreads.remove(Thread.currentThread());
        }
      }));
    }
    beginLatch.await();
    interruptThreads(executor, big);

    Random rand = new Random(System.currentTimeMillis());
    while (!futures.isEmpty()) {
      int loopInterval = 100 + rand.nextInt(1000);
      Future f = futures.remove(rand.nextInt(futures.size()));
      for (int i = 0; i < loopInterval; i++) {
        Thread.yield();
      }
      f.cancel(true);
    }
    endLatch.await();
    if (!writeLocked) {
      fileChannelForWrite.force(true);
      fileChannelForRead.force(true);
      long sz = fileChannelForRead.size();
      assertThat(sz, Matchers.greaterThan(0L));
      ByteBuffer dst = ByteBuffer.allocate(BUF_LEN);
      int numRead = fileChannelForRead.read(dst, sz - BUF_LEN);
      assertThat(numRead, is(BUF_LEN));
      assertArrayEquals(SRC.array(), dst.array());
    }
  }

  private Runnable writeLoopRunner(final long position) {
    return writeLoopRunner(position, false);
  }

  private Runnable writeLoopRunner(final long position, boolean force) {
    return () -> writeLoop(position, force);
  }

  private Runnable readLoopRunner(final long position) {
    return () -> readLoop(position);
  }

  private Runnable scatteredReadLoopRunner(final long position) {
    return () -> scatteredReadLoop(position);
  }

  private Runnable mapAndSizeLoopRunner() {
    return this::mapAndSizeLoop;
  }

  private Runnable lockLoopRunner(final long position, boolean tryLock) {
    return () -> lockLoop(position, tryLock);
  }

  private void writeLoop(long position, boolean force) {
    writeOpener.initLocalReopen();
    do {
      try {
        writeBuf(position, false, force);
      } catch (IOException e) {
        System.out.println("Unexpected exception " + e.getClass().getSimpleName());
        e.printStackTrace();
        fail("Unexpected Exception in writeLoop");
      }
    } while (!writeOpener.isLocalReopen());
  }

  private void readLoop(long position) {
    readOpener.initLocalReopen();
    do {
      try {
        ByteBuffer dst = readBuf(position, false);
        assertArrayEquals(SRC.array(), dst.array());
      } catch (IOException e) {
        System.out.println("Unexpected exception " + e.getClass().getSimpleName());
        e.printStackTrace();
        fail("Unexpected exception in readLoop");
      }
    } while (!readOpener.isLocalReopen());
  }

  private void lockLoop(long position, boolean tryLock) {
    readOpener.initLocalReopen();
    do {
      try {
        FileLock l = (tryLock) ? fileChannelForRead.tryLock(position, BUF_LEN, true) :
            fileChannelForRead.lock(position, BUF_LEN, true);
        ByteBuffer dst = readBuf(position, false);
        assertArrayEquals(SRC.array(), dst.array());
        l.release();
      } catch (IOException e) {
        System.out.println("Unexpected exception " + e.getClass().getSimpleName());
        e.printStackTrace();
        fail("Unexpected exception in lockLoop");
      }
    } while (!readOpener.isLocalReopen());
  }

  private void scatteredReadLoop(long position) {
    readOpener.initLocalReopen();
    do {
      try {
        readScatteredBuf(position);
      } catch (IOException e) {
        System.out.println("Unexpected exception " + e.getClass().getSimpleName());
        e.printStackTrace();
        fail("Unexpected Exception in lockLoop");
      }
    } while (!readOpener.isLocalReopen());
  }

  private void mapAndSizeLoop() {
    readOpener.initLocalReopen();
    do {
      try {
        MappedByteBuffer mapped = fileChannelForRead.map(FileChannel.MapMode.READ_ONLY,0,
            (int)fileChannelForRead.size());
        mapped.force();
        Thread.yield();
        Thread.yield();
        Thread.yield();
        assertThat(mapped.get(100), is(BIG_SRC_BUF[100]));
      } catch (IOException e) {
        System.out.println("Unexpected exception " + e.getClass().getSimpleName());
        e.printStackTrace();
        fail("Unexpected Exception in lockLoop");
      }
    } while (!readOpener.isLocalReopen());
  }

  private void writeBuf(long position, boolean big) throws IOException {
    writeBuf(position, big, false);
  }

  private synchronized void writeBuf(long position, boolean big, boolean force) throws IOException {
    ByteBuffer b = (big) ? BIG_SRC.duplicate() : SRC.duplicate();
    boolean interrupted = Thread.interrupted();
    long pos = position;
    int totalWrites = 0;
    try {
      while (b.hasRemaining()) {
        try {
          if (pos < 0) {
            pos = fileChannelForWrite.position();
          }
          int numWritten = (position >= 0) ? fileChannelForWrite.write(b, position + totalWrites) :
              fileChannelForWrite.write(b);
          if (numWritten < 0) {
            fail("unexpected return");
          }
          totalWrites += numWritten;
          interrupted |= Thread.interrupted();
        } catch (PositionLostException rce) {
          if (pos >= 0) {
            fileChannelForWrite.position(pos + b.position());
          } else {
            fileChannelForWrite.position(0);
            b = (big) ? BIG_SRC.duplicate() : SRC.duplicate();
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      assertThat(totalWrites, is((big) ? BIG_BUF_LEN : BUF_LEN));
      if (force) {
        fileChannelForWrite.force(true);
      }
    }
  }

  private synchronized ByteBuffer readBuf(long position, boolean big) throws IOException {
    ByteBuffer b = ByteBuffer.allocate(big ? BIG_BUF_LEN : BUF_LEN);
    long pos = position;
    boolean interrupted = Thread.interrupted();
    int totalRead = 0;
    try {
      while (b.hasRemaining()) {
        try {
          if (pos < 0) {
            pos = fileChannelForRead.position();
          }
          int numRead = (position >= 0) ? fileChannelForRead.read(b, position + b.duplicate().flip().remaining()) :
              fileChannelForRead.read(b);
          if (numRead < 0) {
            fail("unexpected return");
          }
          totalRead += numRead;
        } catch (PositionLostException rce) {
          if (pos >= 0) {
            fileChannelForRead.position(pos + b.position());
          } else {
            fileChannelForWrite.position(0L);
            b = ByteBuffer.allocate(big ? BIG_BUF_LEN : BUF_LEN);
          }
        }
        interrupted |= Thread.interrupted();
      }
      b.flip();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      assertThat(totalRead, is((big) ? BIG_BUF_LEN : BUF_LEN));
    }
    return b;
  }

  private synchronized void readScatteredBuf(long position) throws IOException {
    int numReads = BIG_BUF_LEN/BUF_LEN;
    ByteBuffer[] dsts = new ByteBuffer[numReads];
    for (int i = 0; i < numReads; i++) {
      dsts[i] = ByteBuffer.allocate(BUF_LEN);
    }
    ByteBuffer dst = ByteBuffer.allocate(BIG_BUF_LEN);
    int offset = 0;
    int len = numReads;
    long pos = position;
    boolean positioned = false;
    boolean interrupted = Thread.interrupted();
    long n;
    try {
      while (dst.hasRemaining() && offset < numReads && len > 0) {
        n = 0;
        try {
          if (!positioned) {
            if (position >= 0) {
              fileChannelForRead.position(position);
              assertThat(fileChannelForRead.size(), greaterThanOrEqualTo((long)BIG_BUF_LEN));
            } else {
              pos = fileChannelForRead.position();
            }
            positioned = true;
          }
          n = fileChannelForRead.read(dsts, offset, len);
          if (n < 0) {
            fail("Unexpected scattered read failure");
          }
          if (n == 0) {
            continue;
          }
          interrupted |= Thread.interrupted();
          assertThat(dst.duplicate().flip().remaining() + n, is(fileChannelForRead.position()));
        } catch (PositionLostException rce) {
          System.out.println("HIT POSITION RESET " + pos + " " + dst.duplicate().flip().remaining() + " " + offset);
          fileChannelForRead.position(pos + dst.position() + n);
          if (n <= 0) {
            continue;
          }
        }
        int i = 0;
        for (ByteBuffer d : dsts) {
            if (i++ < offset) {
              continue;
          }
          ByteBuffer q = d.duplicate();
          q.flip();
          if (q.hasRemaining()) {
            if (q.remaining() >= BUF_LEN || !d.hasRemaining()) {
              byte[] bytes = Arrays.copyOfRange(BIG_SRC_BUF, offset * BUF_LEN, (offset + 1) * BUF_LEN);
              try {
                assertArrayEquals(bytes, q.array());
              } catch (AssertionError e) {
                System.out.println("OFFSET = " + offset + " " + len);
                ByteBuffer q1 = ByteBuffer.wrap(q.array());
                for (int k = 0; k < BUF_LEN/Integer.SIZE; k++) {
                  System.out.printf(" " + q1.getInt());
                }
                throw e;
              }
              dst.put(q);
              offset++;
              len--;
            } else {
              dst.put(q);
              break;
            }
          } else {
            break;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    dst.flip();
    assertThat(dst.remaining(), is(BIG_BUF_LEN));
    assertThat(dst.array().length, is(BIG_BUF_LEN));
    assertArrayEquals(BIG_SRC.array(), dst.array());
  }

  private void interruptThreads(ExecutorService executor, final boolean big) {
    executor.submit(() -> {
      Random rand = new Random(System.currentTimeMillis());
      int k = 0;
      int max = 100;
      while (!runningThreads.isEmpty()) {
        Thread t;
        try {
          t = runningThreads.get(rand.nextInt(runningThreads.size()));
        } catch (IndexOutOfBoundsException ignored) {
          continue;
        }
        for (int i = 0; i < max; i++) {
          t.interrupt();
          Thread.yield(); Thread.yield(); Thread.yield();
        }
        int sleepTime;
        if (++k % 10 == 0) {
          max = 1000;
          sleepTime = (big) ? rand.nextInt(200) : rand.nextInt(100);
        } else {
          max = 100;
          sleepTime = (big) ? rand.nextInt(50) : rand.nextInt(10);
        }
        // pause for sometime before issuing next interrupts
        for (int i = 0; i < 10 + sleepTime; i++) {
          try {
            Thread.sleep(1);
            if (runningThreads.isEmpty()) {
              break;
            }
          } catch (InterruptedException ignored) {
          }
        }
      }
    });
  }

  private static final class TestReadChannelOpener extends FileChannelReadOpener {
    private final AtomicInteger reopenHappenedCount;
    private final ThreadLocal<Boolean> localReopen;

    TestReadChannelOpener(File fileToOpen) {
      super(fileToOpen);
      this.reopenHappenedCount = new AtomicInteger(0);
      this.localReopen = new ThreadLocal<>();
    }

    @Override
    public FileChannel reopen() throws IOException {
      reopenHappenedCount.incrementAndGet();
      localReopen.set(true);
      return super.reopen();
    }

    private int getReopenHappenedCount() {
      return reopenHappenedCount.get();
    }

    private void initLocalReopen() {
      localReopen.set(false);
    }

    private boolean isLocalReopen() {
      return localReopen.get();
    }
  }

  private static final class TestWriteChannelOpener extends FileChannelWriteOpener {
    private final AtomicInteger reopenHappenedCount;
    private final ThreadLocal<Boolean> localReopen;

    TestWriteChannelOpener(File fileToOpen) {
      super(fileToOpen);
      reopenHappenedCount = new AtomicInteger(0);
      this.localReopen = new ThreadLocal<>();
    }

    @Override
    public FileChannel reopen() throws IOException {
      reopenHappenedCount.incrementAndGet();
      localReopen.set(true);
      return super.reopen();
    }

    private int getReopenHappenedCount() {
      return reopenHappenedCount.get();
    }

    private void initLocalReopen() {
      localReopen.set(false);
    }

    private boolean isLocalReopen() {
      return localReopen.get();
    }
  }
}
