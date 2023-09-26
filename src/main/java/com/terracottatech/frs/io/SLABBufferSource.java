/*
 * Copyright (c) 2014-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class SLABBufferSource implements BufferSource {

  public static final int HEADERSZ = 4;
  private final int slabsize;
  private final int maxslabs;
  private SLAB[] list;
  private volatile int pointer = 0;
  private static final Logger LOGGER = LoggerFactory.getLogger(SLABBufferSource.class);
  private final AtomicInteger out = new AtomicInteger();

  public SLABBufferSource() {
    this(8 * 1024 * 1024, 128);
  }

  public SLABBufferSource(int maxSize) {
    this(8 * 1024 * 1024, maxSize / (8 * 1024 * 1024));
  }

  public SLABBufferSource(int slabsize, int maxslabs) {
    this.slabsize = slabsize;
    if (maxslabs <= 0) {
      maxslabs = 1;
    }
    this.maxslabs = maxslabs;
    int startslabs = this.maxslabs / 4;
    if (startslabs == 0) {
      startslabs = this.maxslabs;
    }
    this.list = new SLAB[startslabs];
  }

  public int getSlabSize() {
    return slabsize;
  }

  public int getSize() {
    int total = 0;
    for (SLAB s : list) {
      if (s != null) {
        total += slabsize;
      }
    }
    return total;
  }

  public int verify() {
    return out.get();
  }

  private int amountRemaining() {
    int total = 0;
    for (SLAB s : list) {
      if (s != null) {
        total += s.remaining();
      }
    }
    return total;
  }

  private boolean isCapacityAvailable(int size) {
    int sizeToCheck = size + SLABBufferSource.HEADERSZ;
    for (SLAB s : list) {
      if (s != null) {
        if (s.remaining() >= sizeToCheck) {
          return true;
        }
      } else {
        // if there are empty slots as the capacity may increase..return true
        return true;
      }
    }
    return false;
  }

  private int count() {
    int total = 0;
    for (SLAB s : list) {
      if (s != null) {
        total += s.count();
      }
    }
    return total;
  }

  private int point(int size) throws OutOfDirectMemoryException {
    boolean sweep = false;
    for (int x = pointer; x < list.length; x++) {
      if (list[x] == null) {
        if (sweep) {
          if (!installSlab(x)) {
            sweep = false;
            x -= 1;
            continue;
          }
        } else {
          sweep = true;
          x = -1;
          continue;
        }
      }

      if (list[x].reserve(size)) {
        if (list[pointer].remaining() < size) {
          if (pointer != x) {
            LOGGER.debug("POINTER: " + pointer + " " + x);
          }
          pointer = x;
        }
        return x;
      } else if (x + 1 == list.length && !sweep) {
        sweep = true;
        x = -1;
      }
    }
    return (expandRange()) ? -1 : -2;
  }

  private synchronized boolean expandRange() {
    if (list.length == maxslabs) {
      return false;
    }
    int newsize = list.length * 2;
    if (newsize > maxslabs) {
      newsize = maxslabs;
    }
    this.list = Arrays.copyOf(list, newsize);
    return true;
  }

  private synchronized boolean installSlab(int position) throws OutOfDirectMemoryException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("allocating a new slab with " + amountRemaining());
    }
    if (list[position] == null) {
      list[position] = new SLAB(position, slabsize);
      return true;
    }

    return false;

  }

//  private ByteBuffer allocateSoleSlab(int size) {
//    ByteBuffer b = ByteBuffer.allocateDirect(size + HEADERSZ);
//    b.putInt(-1);
//    return b;
//  }
  @Override
  public ByteBuffer getBuffer(int size) {
    SLAB target = null;
    if (size > slabsize) {
      return null;
//      return allocateSoleSlab(size);
    }
    try {
      while (target == null) {
        int p = point(size);
        if (p >= 0) {
          target = list[p];
          if (target.getId() != p) {
            throw new AssertionError("bad pointer");
          }
          out.incrementAndGet();
          return target.allocateReserved(size);
        } else if (p < -1 && (amountRemaining() < slabsize || !isCapacityAvailable(size))) {
          return null;
        }
      }
    } catch (OutOfDirectMemoryException oome) {
      return null;
    }
    return null;
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    int pos = buffer.getInt(0);
    if (pos < 0) {
      return;
    }
    if (pos >= list.length) {
      throw new AssertionError(pos + " " + buffer.capacity());
    }
    SLAB parent = list[pos];
    if (parent.getId() != pos) {
      throw new AssertionError("bad pointer");
    }
    if (out.decrementAndGet() == 0) {
      if (count() == 0) {
        throw new AssertionError();
      }
    }
    if (parent.release(buffer)) {

    }
  }

  @Override
  public void reclaim() {
    for (SLAB b : list) {
      if (b != null) {
        b.clear();
      }
    }
    out.set(0);
  }

  @Override
  public String toString() {
    int x = 0;
    for (x = 0; x < list.length; x++) {
      if (list[x] == null) {
        break;
      }
    }
    return "SLABBufferSource{" + "slabsize=" + slabsize + ", maxslabs=" + maxslabs + ", list=" + x + ", pointer=" + pointer + ", out=" + out + '}';
  }

  private static class SLAB extends ReentrantLock {

    private int reserve = 0;
    private final int id;
    private int count = 0;
    private final ByteBuffer buffer;

    SLAB(int id, int slabsize) throws OutOfDirectMemoryException {
      try {
        buffer = ByteBuffer.allocateDirect(slabsize);
      } catch ( OutOfMemoryError oome ) {
// assume direct memory oome
        throw new OutOfDirectMemoryException(oome);
      }
      this.id = id;
    }

    public boolean reserve(int size) {
      size += SLABBufferSource.HEADERSZ;
      if (tryLock()) {
        try {
          if (count == 0) {
            buffer.clear();
          }
          if (buffer.remaining() - reserve - size >= 0) {
            reserve += size;
            LOGGER.debug("RESERVE WIN: " + id + " " + " " + size);
            return true;
          }
          LOGGER.debug("RESERVE FAIL: no reserve " + id + " " + " " + count);
        } finally {
          unlock();
        }
      } else {
        LOGGER.debug("RESERVE FAIL: unable to lock " + " " + id + " " + " " + count);
      }
      return false;

    }

    public boolean release(ByteBuffer buffer) {
      lock();
      try {
        if (buffer.getInt(0) != id) {
          throw new AssertionError();
        }
        return (--count == 0);
      } finally {
        unlock();
      }
    }

    public ByteBuffer allocateReserved(int size) {
      size += HEADERSZ;
      lock();
      try {
        if (count == 0) {
          buffer.clear();
        }
        if (size <= reserve) {
          reserve -= size;
          buffer.limit(buffer.position() + size);
          ByteBuffer child = buffer.slice();
          buffer.position(buffer.limit()).limit(buffer.capacity());
          child.putInt(id);
          count++;
          return child;
        }
      } finally {
        unlock();
      }
      throw new RuntimeException("allocation not reserved first");
    }

    public void clear() {
      count = 0;
      reserve = 0;
      buffer.clear();
    }

    public int remaining() {
      return buffer.remaining() - reserve;
    }

    public int getId() {
      return id;
    }

    public int count() {
      return count;
    }

  }
  
  private static class OutOfDirectMemoryException extends Exception {

    public OutOfDirectMemoryException(Throwable cause) {
      super(cause);
    }
    
  }

}
