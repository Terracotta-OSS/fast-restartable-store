/*
 * Copyright (c) 2019-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

public class Log2LatencyBinsTest {
  @Test
  public void testCreation() {
    {
      // simple one
      Log2LatencyBins lb = new Log2LatencyBins("test1", 10, 16);
      assertThat(lb.getName(), is("test1"));
      assertThat(lb.nanosMaxForBin(0), is(1l << 16));
      assertThat(lb.nanosMaxForBin(8), is(1l << 24));
      assertThat(lb.nanosMaxForBin(9), is(Long.MAX_VALUE));
      assertThat(lb.getBinCount(), is(10));
    }
    {
      // null name
      try {
        Log2LatencyBins lb = new Log2LatencyBins(null, 10, 16);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
    {
      // oversized bincount
      Log2LatencyBins lb = new Log2LatencyBins("test", 100, 16);
      assertThat(lb.getBinCount(), is(32));
      assertThat(lb.getSmallestResolutionPower(), is(16));
    }
    {
      // undersized bincount
      Log2LatencyBins lb = new Log2LatencyBins("test", 0, 16);
      assertThat(lb.getBinCount(), is(2));
      assertThat(lb.getSmallestResolutionPower(), is(16));
    }
    {
      // undersized power.
      Log2LatencyBins lb = new Log2LatencyBins("test", 10, 1);
      assertThat(lb.getBinCount(), is(10));
      assertThat(lb.getSmallestResolutionPower(), is(2));
    }
    {
      // oversized power
      Log2LatencyBins lb = new Log2LatencyBins("test", 20, 50);
      assertThat(lb.getBinCount(), is(20));
      assertThat(lb.getSmallestResolutionPower(), is(43));
    }
    {
      // bad max call
      Log2LatencyBins lb = new Log2LatencyBins("test", 10, 16);
      try {
        lb.nanosMaxForBin(20);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
    {
      // bad max call
      Log2LatencyBins lb = new Log2LatencyBins("test", 10, 16);
      try {
        lb.nanosMaxForBin(-1);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
    {
      // bad min call
      Log2LatencyBins lb = new Log2LatencyBins("test", 10, 16);
      try {
        lb.nanosMinForBin(20);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
    {
      // bad min call
      Log2LatencyBins lb = new Log2LatencyBins("test", 10, 16);
      try {
        lb.nanosMinForBin(-1);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }
  }

  @Test
  public void testRecording() {
    Log2LatencyBins lb = new Log2LatencyBins("test1", 10, 16);
    for (int i = 0; i < 32; i++) {
      lb.record((1L << i) - 1);
    }
    long[] bcounts = lb.binCounts();

    assertThat(bcounts[0], is(18l));
    assertThat(bcounts[9], is(6l));
    for (int i = 1; i < 9; i++) {
      assertThat(bcounts[i], is(1l));
    }
    assertThat(lb.count(), is(32l));
    System.out.println(lb.toString());
  }

  @Test
  public void testSingleCallback() {
    Log2LatencyBins lb = new Log2LatencyBins("test1", 10, 16);
    final AtomicBoolean seen = new AtomicBoolean(false);
    Runnable runnableIs = lb.reporterRunnable(() -> false, 0, TimeUnit.SECONDS, -1, (lb2) -> {
      assertThat(lb2, is(lb));
      seen.set(true);
    });
    runnableIs.run();
    assertThat(seen.get(), is(true));
  }

  @Test
  public void testMultiCallback() throws InterruptedException {
    Log2LatencyBins lb = new Log2LatencyBins("test1", 10, 16);
    final AtomicInteger seen = new AtomicInteger(0);
    Thread t = lb.reporterThread(() -> (seen.get() < 5), 10, TimeUnit.MILLISECONDS, -1, (lb2) -> {
      seen.incrementAndGet();
      assertThat(lb2, is(lb));
    });
    t.start();
    t.join();
    assertThat(seen.get(), is(5));
  }

  @Test
  public void testInterruptWorksk() throws InterruptedException {
    Log2LatencyBins lb = new Log2LatencyBins("test1", 10, 16);
    final AtomicInteger seen = new AtomicInteger(0);

    Thread t = lb.reporterThread(() -> (seen.get() < Integer.MAX_VALUE), 100000, TimeUnit.MILLISECONDS, -1, (lb2) -> {
      seen.incrementAndGet();
      assertThat(lb2, is(lb));
    });
    t.start();
    Thread.sleep(100);
    seen.set(Integer.MAX_VALUE);
    t.interrupt();
    t.join();
    assertThat(seen.get(), lessThan(Integer.MAX_VALUE));
  }
}