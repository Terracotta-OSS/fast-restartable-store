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
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class HiLoBufferSource implements BufferSource {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(HiLoBufferSource.class);
  
  private final int threshold;
  private final AtomicInteger allocations = new AtomicInteger();
  private final SplittingBufferSource lo;
  private final SLABBufferSource      hi;
  private final CachingBufferSource          hiCache;
  public static final int DEFAULTSLAB = 8 * 1024 * 1024;
  
  public HiLoBufferSource(int totalsize) {
    this(DEFAULTSLAB, totalsize);
  }
  
  public HiLoBufferSource(int slabsize, int totalsize) {
    this(slabsize/32, slabsize, totalsize);
  }
  
  public HiLoBufferSource(int threshold, int slabsize, int totalsize) {
    if ( slabsize > totalsize / 4 ) {
      slabsize = totalsize / 4;
      LOGGER.warn("slab size for memory is greater than 25% of total size.  Adjusting slabsize to 25% of total size or " + slabsize + " bytes");
    }
    if ( threshold >= slabsize / 4 ) {
      threshold = slabsize / 4;
      LOGGER.warn("threshold size for memory is greater than 25% of slab size.  Adjusting threshold to 25% of slabsize or " + threshold + " bytes");
    }

    this.threshold = threshold;
    int maxslabs = (totalsize / slabsize) - 1;
    lo = new SplittingBufferSource(32, slabsize);
    hi = new SLABBufferSource(slabsize, maxslabs/2);
    hiCache = new LimitedCachingBufferSource((maxslabs/2 * slabsize) - slabsize);
  }
  
  HiLoBufferSource(int threshold, BufferProvider provider) {
    hiCache = provider.getLargeCache();
    hi = provider.getLargeSource();
    lo = provider.getSmallSource();
    this.threshold = threshold;
  }

  @Override
  public ByteBuffer getBuffer(int size) {
    if ( allocations.incrementAndGet() % 8196 == 0 && LOGGER.isDebugEnabled() ) {
      LOGGER.debug("allocations=" + allocations.get() + " usage=" + this.usage());
    }
    ByteBuffer buffer = null;
    if ( size < threshold ) {
      buffer = lo.getBuffer(size);
    }
    if ( buffer == null ) {
      if ( size + SLABBufferSource.HEADERSZ >= hi.getSlabSize() ) {
        buffer = hiCache.getBuffer(size + SLABBufferSource.HEADERSZ);
        if ( buffer == null ) {
          buffer = largeAllocation(size + SLABBufferSource.HEADERSZ);
        }
        if ( buffer != null ) {
          buffer.putInt(Integer.MIN_VALUE);
        }
      } else {
        buffer = hi.getBuffer(size);
        if ( buffer != null ) {
          buffer.putInt(0, buffer.getInt(0)|0x80000000);
        }
      } 
    }
    return buffer;
  }
  
  private synchronized ByteBuffer largeAllocation(int size) {
    if ( size < 0 ) {
      return null;
    }
    try {
      return ByteBuffer.allocateDirect(size);
    } catch ( OutOfMemoryError oome ) {
      return null;
    }
  }

  @Override
  public void returnBuffer(ByteBuffer buffer) {
    if ( buffer.getInt(0) < 0 ) {
      if ( buffer.capacity() > hi.getSlabSize() ) {
        if ( buffer.getInt(0) != Integer.MIN_VALUE ) {
          throw new AssertionError();
        }
        hiCache.returnBuffer(buffer);
      } else {
        buffer.putInt(0,buffer.getInt(0)&0x7fffffff);
        hi.returnBuffer(buffer);
      }
    } else {
      lo.returnBuffer(buffer);
    } 
  }

  @Override
  public void reclaim() {
    hi.reclaim();
    lo.reclaim();
    hiCache.reclaim();
  }
  
  public long usage() {
    return lo.usage() + hi.getSize() + hiCache.getSize();
  }

  @Override
  public String toString() {
    return "HiLoBufferSource{used=" + usage() + ", threshold=" + threshold + ", \nlo=" + lo + ", \nhi=" + hi + ", \nhiCache=" + hiCache + '}';
  }
  
  interface BufferProvider {
    public SplittingBufferSource getSmallSource();
    public SLABBufferSource getLargeSource();
    public CachingBufferSource getLargeCache();
  }
}
