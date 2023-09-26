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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Abstraction above {@link FileChannel} to build a re-openable file channel in case of interrupts.
 * <p>
 * For all calls that depends on the current file position there is a very tiny possibility that an
 * {@link PositionLostException} is thrown, iff the call is interrupted. Callers can optionally catch this exception to
 * restore the lost position and reuse the channel.
 *
 * <pre> {@code
 *    ....
 *    ByteBuffer buf = ...
 *    FileChannel source = new WrappedFileChannel(delegate, opener);
 *    startPosition = buf.position;
 *    while (buf.hasRemaining()) {
 *      try {
 *        source.read(buf);
 *      } catch (PositionLostException ple) {
 *        source.position(buf.position - startPosition);
 *      }
 *    }
 * }
 * </pre>
 */
public class WrappedFileChannel extends FileChannel {
  private final ChannelOpener channelOpener;
  private final Set<WrappedFileLock> grantedLocks;
  private final ReentrantLock posLock;
  private final AtomicInteger threadsInChannelMethod;
  private volatile FileChannel channel;
  private volatile boolean positionLost;
  private int threadsInFileLock;

  public WrappedFileChannel(FileChannel channel, ChannelOpener channelOpener) {
    this.channelOpener = channelOpener;
    this.channel = channel;
    this.grantedLocks = Collections.newSetFromMap(new ConcurrentHashMap<WrappedFileLock, Boolean>());
    this.threadsInFileLock = 0;
    this.posLock = new ReentrantLock();
    this.positionLost = false;
    this.threadsInChannelMethod = new AtomicInteger(0);
  }

  @Override
  public int read(final ByteBuffer dst) throws IOException {
    return retryOnInterruptWithBufPosAndFilePos(FileChannel::read, dst);
  }

  @Override
  public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
    return retryOnInterruptWithBufPosAndFilePos((c, b) -> c.read(b, offset, length), dsts);
  }

  @Override
  public int write(final ByteBuffer src) throws IOException {
    return retryOnInterruptWithBufPosAndFilePos(FileChannel::write, src);
  }

  @Override
  public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
    return retryOnInterruptWithBufPosAndFilePos((c, b) -> c.write(b, offset, length), srcs);
  }

  @Override
  public long position() throws IOException {
    return retryOnInterruptIfPosNotLost(FileChannel::position);
  }

  @Override
  public FileChannel position(final long newPosition) throws IOException {
    // position is regained even in a switched channel, if this method succeeds
    retryOnInterruptLockPosAndPosGain(c -> c.position(newPosition));
    return this;
  }

  @Override
  public long size() throws IOException {
    return retryOnInterrupt(FileChannel::size);
  }

  @Override
  public FileChannel truncate(final long size) throws IOException {
    // position may be gained only if truncate truncates less than current position. so assume worst case here
    retryOnInterruptLockPos(c -> c.truncate(size));
    return this;
  }

  @Override
  public void force(final boolean metaData) throws IOException {
    retryOnInterrupt(FileChannel::force, metaData);
  }

  @Override
  public long transferTo(final long position, final long count, final WritableByteChannel target) throws IOException {
    return retryOnChannelSwitch(c -> c.transferTo(position, count, target), null, null);
  }

  @Override
  public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {
    // interrupt handling not supported due to two channels
    return retryOnChannelSwitch(c -> c.transferFrom(src, position, count), null, null);
  }

  @Override
  public int read(final ByteBuffer dst, final long position) throws IOException {
    return retryOnInterruptWithBufPos((c, b) -> c.read(b, position), dst);
  }

  @Override
  public int write(final ByteBuffer src, final long position) throws IOException {
    return retryOnInterruptWithBufPos((c, b) -> c.write(b, position), src);
  }

  @Override
  public MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {
    return retryOnInterrupt(c -> c.map(mode, position, size));
  }

  @Override
  public FileLock lock(final long position, final long size, final boolean shared) throws IOException {
    // interrupt handling not supported due to locks being left around on interrupt
    boolean interrupted = Thread.interrupted();
    boolean exited = false;
    try {
      while (true) {
        exited = false;
        enterLock();
        try {
          FileLock l = channel.lock(position, size, shared);
          if (l != null) {
            WrappedFileLock wl = new WrappedFileLock(this, l);
            grantedLocks.add(wl);
            return wl;
          }
          return null;
        } catch (ClosedChannelException | FileLockInterruptionException cce) {
          // need to exit lock before reopening
          exited = true;
          exitLock();
          interrupted |= reopen(cce);
        }
      }
    } finally {
      if (!exited) {
        exitLock();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
    try {
      enterLock();
      FileLock l = retryOnChannelSwitch(c -> c.tryLock(position, size, shared), this::exitLock, this::enterLock);
      if (l != null) {
        WrappedFileLock wl = new WrappedFileLock(this, l);
        grantedLocks.add(wl);
        return wl;
      }
      return null;
    } finally {
      exitLock();
    }
  }

  @Override
  protected void implCloseChannel() throws IOException {
    channel.close();
  }

  private <R> R retryOnInterruptWithBufPosAndFilePos(ChannelBiFunction<R, ByteBuffer> actionToTake, ByteBuffer buf) throws IOException {
    return retryOnInterruptWithPos(true, actionToTake, buf, new ByteBuffer[] {buf});
  }

  private <R> R retryOnInterruptWithBufPosAndFilePos(ChannelBiFunction<R, ByteBuffer[]> actionToTake, ByteBuffer[] bufs) throws IOException {
    return retryOnInterruptWithPos(true, actionToTake, bufs, bufs);
  }

  private <R> R retryOnInterruptWithBufPos(ChannelBiFunction<R, ByteBuffer> actionToTake, ByteBuffer buf) throws IOException {
    return retryOnInterruptWithPos(false, actionToTake, buf, new ByteBuffer[] {buf});
  }

  private <R, U> R retryOnInterruptWithPos(boolean savePosition, ChannelBiFunction<R, U> actionToTake, U u, ByteBuffer[] bufs) throws IOException {
    if (channelOpener.isClosed()) {
      throw new ClosedChannelException();
    }
    boolean interrupted = Thread.interrupted();
    int[] bufPositions = new int[bufs.length];
    long pos = -1;

    int i = 0;
    for (ByteBuffer buf : bufs) {
      bufPositions[i++] = buf.position();
    }
    threadsInChannelMethod.incrementAndGet();
    final ReentrantLock lock = posLock;
    if (savePosition) {
      lock.lock();
    }
    try {
      while (true) {
        FileChannel currentChannel = channel;
        if (this.positionLost && savePosition) {
          // there is no way to recover the position on a switched channel in this case. See if the caller can
          // do something about this.
          throw new PositionLostException();
        }
        try {
          if (savePosition && pos < 0) {
            // if this is true, assumption is lock is held.
            pos = currentChannel.position();
          }
          return actionToTake.apply(currentChannel, u);
        } catch (ClosedChannelException cce) {
          interrupted |= reopen(cce, pos);
          i = 0;
          for (ByteBuffer b : bufs) {
            b.position(bufPositions[i++]);
          }
        }
      }
    } finally {
      if (savePosition) {
        lock.unlock();
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      threadsInChannelMethod.decrementAndGet();
    }
  }

  private <R> void retryOnInterrupt(ChannelBiConsumer<R> actionToTake, R r) throws IOException {
    retryOnInterrupt((ChannelFunction<Void>) (c) -> {
      actionToTake.accept(channel, r);
      return null;
    });
  }

  private <R> R retryOnInterrupt(ChannelFunction<R> actionToTake) throws IOException {
    return retryOnInterrupt(actionToTake, false, false, false);
  }

  private <R> R retryOnInterruptIfPosNotLost(ChannelFunction<R> actionToTake) throws IOException {
    return retryOnInterrupt(actionToTake, false, false, true);
  }

  private <R> R retryOnInterruptLockPos(ChannelFunction<R> actionToTake) throws IOException {
    return retryOnInterrupt(actionToTake, true, false, false);
  }

  private <R> R retryOnInterruptLockPosAndPosGain(ChannelFunction<R> actionToTake) throws IOException {
    return retryOnInterrupt(actionToTake, true, true, false);
  }

  private <R> R retryOnInterrupt(ChannelFunction<R> actionToTake, boolean lockPosition, boolean posGained,
                                 boolean posImportant) throws IOException {
    threadsInChannelMethod.incrementAndGet();
    boolean success = false;
    final ReentrantLock lock = posLock;
    FileChannel appliedChannel = channel;
    if (lockPosition) {
      lock.lock();
    }
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        if (posImportant && this.positionLost) {
          throw new PositionLostException();
        }
        try {
          appliedChannel = channel;
          R r = actionToTake.apply(appliedChannel);
          success = true;
          return r;
        } catch (ClosedChannelException | FileLockInterruptionException cce) {
          interrupted |= reopen(cce);
        }
      }
    } finally {
      threadsInChannelMethod.decrementAndGet();
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      if (lockPosition) {
        lock.unlock();
      }
      if (success && posGained) {
        if (channel == appliedChannel && this.positionLost) {
          synchronized (this) {
            if (channel == appliedChannel) {
              // we have regained the position if channel has not switched from underneath us
              this.positionLost = false;
            }
          }
        }
      }
    }
  }

  private <R> R retryOnChannelSwitch(ChannelFunction<R> actionToTake, Runnable before, Runnable after) throws IOException {
    FileChannel usedChannel = channel;
    // mask of any pending interrupts before entering the call
    boolean interrupted = Thread.interrupted();
    try {
      while (true) {
        try {
          return actionToTake.apply(usedChannel);
        } catch (ClosedChannelException cce) {
          if (channelOpener.isClosed()) {
            throw cce;
          }
          interrupted |= Thread.interrupted();
          if (before != null) {
            before.run();
          }
          try {
            boolean channelSwitched = false;
            while (!channelSwitched && threadsInChannelMethod.get() > 0) {
              // someone should switch
              synchronized (this) {
                if (usedChannel != channel) {
                  usedChannel = channel;
                  channelSwitched = true;
                }
              }
              // yield so that if there is any thread trying to reopen and switch channel
              Thread.yield();
              Thread.yield();
            }
            if (!channelSwitched) {
              throw cce;
            }
          } finally {
            if (after != null) {
              after.run();
            }
          }
        }
      }
    } finally {
      // now unmask the interrupt
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private synchronized void releaseLock(WrappedFileLock releasedLock) throws IOException {
    releasedLock.actual.release();
    grantedLocks.remove(releasedLock);
  }

  private synchronized void enterLock() {
    this.threadsInFileLock++;
  }

  private synchronized void exitLock() {
    this.threadsInFileLock--;
    if (this.threadsInFileLock == 0) {
      this.notifyAll();
    }
  }


  private boolean reopen(IOException ioe) throws IOException {
    return reopen(ioe, -1);
  }

  private boolean reopen(IOException ioe, long pos) throws IOException {
    if (channelOpener.isClosed()) {
      // explicitly closed
      throw ioe;
    }
    boolean interrupted = Thread.interrupted();
    FileChannel previous = channel;
    synchronized (this) {
      if (previous == channel) {
        // this means we as a thread is responsible to reopen the channel
        // and we prevent multiple reopens. Prepare the new channel
        // by grabbing locks before allowing other threads to access this new channel
        FileChannel tmpChannel;
        do {
          try {
            tmpChannel = channelOpener.reopen();
            if (pos >= 0) {
              tmpChannel.position(pos);
            }
            interrupted |= reAcquireGrantedLocks(tmpChannel);
          } catch (ClosedChannelException cce) {
            if (channelOpener.isClosed()) {
              throw cce;
            }
            tmpChannel = null;
            interrupted |= Thread.interrupted();
          }
        } while (tmpChannel == null);
        // swap atomically after updating whether position is lost
        this.positionLost = pos < 0;
        this.channel = tmpChannel;
      } // otherwise, this is just a cache refresh for this thread, try using that
    }
    return interrupted;
  }

  private boolean reAcquireGrantedLocks(FileChannel tmpChannel) throws IOException {
    boolean interrupted = Thread.interrupted();
    while (threadsInFileLock > 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        interrupted |= Thread.interrupted();
      }
    }
    interrupted |= Thread.interrupted();
    if (grantedLocks.isEmpty()) {
      return interrupted;
    }
    for (WrappedFileLock wl : grantedLocks) {
      try {
        wl.actual.release();
      } catch (IOException ignored) {
        // ignore
      }
      FileLock l = tmpChannel.tryLock(wl.position(), wl.size(), wl.isShared());
      if (l == null) {
        // this should not happen..if it happens we must let the user know.
        throw new IOException("Unable to relock on the new channel");
      }
      wl.updateLock(l);
    }
    return interrupted | Thread.interrupted();
  }

  /**
   * Duplicate the {@link Function} interface here for channel usage as currently the Function interface does not
   * allow throwing checked exceptions.
   *
   * @param <R> resultant type.
   */
  @FunctionalInterface
  private interface ChannelFunction<R> {
    R apply(FileChannel channel) throws IOException;
  }

  /**
   * Duplicate the {@link java.util.function.Consumer} interface here for channel usage as currently the Consumer
   * interface does not allow throwing checked exceptions.
   *
   * @param <R> second input type
   */
  @FunctionalInterface
  private interface ChannelBiConsumer<R> {
    void accept(FileChannel channel, R r) throws IOException;
  }

  /**
   * Duplicate the {@link java.util.function.BiFunction} interface here for channel usage as currently the
   * {@code BiFunction} interface does not allow throwing checked exceptions.
   *
   * @param <R> resultant type.
   * @param <U> second input type.
   */
  @FunctionalInterface
  private interface ChannelBiFunction<R, U> {
    R apply(FileChannel channel, U u) throws IOException;
  }

  private static final class WrappedFileLock extends FileLock {
    private volatile FileLock actual;
    private final WrappedFileChannel lockedChannel;

    WrappedFileLock(WrappedFileChannel lockedChannel, FileLock actual) {
      super(lockedChannel, actual.position(), actual.size(), actual.isShared());
      this.actual = actual;
      this.lockedChannel = lockedChannel;
    }

    @Override
    public boolean isValid() {
      return actual.isValid();
    }

    @Override
    public void release() throws IOException {
      boolean interrupted = Thread.interrupted();
      try {
        while (true) {
          try {
            lockedChannel.releaseLock(this);
            return;
          } catch (ClosedChannelException e) {
            interrupted |= lockedChannel.reopen(e);
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private void updateLock(FileLock newLock) {
      this.actual = newLock;
    }
  }
}