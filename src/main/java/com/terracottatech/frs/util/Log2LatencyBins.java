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
package com.terracottatech.frs.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * This class tracks latencies in nanoseconds by basically taking the log base 2
 * to track them. You pick a power of 2 as the smallest resolution, like 65536ns,
 * and then it tracks the specified number of bins higher than that, with
 * values doubling for each bin.
 */
public class Log2LatencyBins {

  /**
   * Default smallest resolution power. 2^(this value) is the
   * smallest bin maximum size.
   */
  public static final int DEFAULT_SMALLEST_RESOLUTION = 16;

  /**
   * Default number oif bins to allocate.
   */
  public static final int DEFAULT_BIN_COUNT = 20;
  private final LongAdder[] bins;
  private final LongAdder counter = new LongAdder();
  private final String name;
  private final int binCount;
  private final int smallestResolutionPower;

  public enum ToString {
    NO_RANGES_AND_ZEROS(false, true),
    RANGES_AND_ZEROS(true, true),
    RANGES_NO_ZEROS(true, false);

    private final boolean includeRanges;
    private final boolean includeZeros;

    ToString(boolean includeRanges, boolean includeZeros) {
      this.includeRanges = includeRanges;
      this.includeZeros = includeZeros;
    }

    public boolean includeRanges() {
      return includeRanges;
    }

    public boolean includeZeros() {
      return includeZeros;
    }

  }

  /**
   * Provisions a default Latency bin.
   *
   * @param name
   */
  public Log2LatencyBins(String name) {
    this(name, DEFAULT_BIN_COUNT, DEFAULT_SMALLEST_RESOLUTION);
  }

  /**
   * Create a set of bins for recording latencies.
   *
   * @param name Name of this bin.
   * @param binCount Number of power of 2 bins to allocate. Will be capped 27&lt;=x&lt;=32
   * @param smallestResolutionPower 2^^this arg is the smallest resolution that will
   * be tracked; the increasing bins will each double in span. Will be capped to
   * max(63 - capped binCount, 2).
   */
  public Log2LatencyBins(String name, int binCount, int smallestResolutionPower) {
    if (name == null) {
      throw new IllegalArgumentException("Must have name.");
    }
    this.name = name;
    this.binCount = Math.max(2, Math.min(32, binCount));
    this.smallestResolutionPower = Math.max(2, Math.min(smallestResolutionPower, 63 - this.binCount));
    bins = new LongAdder[this.binCount];
    for (int i = 0; i < bins.length; i++) {
      bins[i] = new LongAdder();
    }
  }

  /**
   * Record some latencies.
   *
   * @param latenciesInNanos array of latencies to record.
   */
  public void record(long... latenciesInNanos) {
    for (long latency : latenciesInNanos) {
      bins[binIndexFor(latency)].increment();
      counter.increment();
    }
  }

  private int binIndexFor(long latencyInNanos) {
    // nicely, leading zeros gets turned into an intrinsic on later JVMs/x86. Fast fast.
    int leadingZeros = 63 - Long.numberOfLeadingZeros(latencyInNanos) - smallestResolutionPower;
    if (leadingZeros <= 0) {
      return 0;
    } else {
      return Math.min(bins.length - 1, leadingZeros);
    }
  }

  public int getSmallestResolutionPower() {
    return smallestResolutionPower;
  }

  @Override
  public String toString() {
    return toString(ToString.NO_RANGES_AND_ZEROS);
  }

  public String toString(ToString formatting) {
    return toString(formatting, binCounts(), count());
  }

  public String toString(ToString formatting, long[] binCounts, long count) {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append(" latencies: (");
    sb.append(count);
    if (!formatting.includeRanges()) {
      sb.append(" @ ");
      sb.append(niceDuration(nanosMaxForBin(0)));
    }
    sb.append(") [");
    boolean first = true;
    for (int i = 0; i < binCounts.length; i++) {
      if (formatting.includeZeros() || binCounts[i] > 0) {
        if (!first) {
          sb.append(", ");
        } else {
          first = false;
        }
        if (formatting.includeRanges()) {
          long maxNS = nanosMaxForBin(i);
          sb.append(binCounts[i]);
          sb.append(" <=");
          sb.append(niceDuration(maxNS));
        } else {
          sb.append(binCounts[i]);
        }
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Get a non coherent snapshot of the bincounts.
   *
   * @return
   */
  public long[] binCounts() {
    long[] ret = new long[bins.length];
    for (int i = 0; i < bins.length; i++) {
      ret[i] = bins[i].longValue();
    }
    return ret;
  }

  /**
   * Get a count of events seen, non coherent with respect to bin
   * counts.
   *
   * @return
   */
  public long count() {
    return counter.longValue();
  }

  public static String niceDuration(long ns) {
    if (ns < TimeUnit.MILLISECONDS.toNanos(1) - 1) {
      // less than 1 milli, bare nanos
      return ns + "ns";
    } else if (ns < TimeUnit.SECONDS.toNanos(1) - 1) {
      // millis
      return TimeUnit.NANOSECONDS.toMillis(ns) + "ms";
    } else if (ns < TimeUnit.MINUTES.toNanos(1) - 1) {
      // seconds.
      return TimeUnit.NANOSECONDS.toSeconds(ns) + "sec";
    } else if (ns < TimeUnit.HOURS.toNanos(1) - 1) {
      // mins
      return TimeUnit.NANOSECONDS.toMinutes(ns) + "min";
    } else if (ns < TimeUnit.DAYS.toNanos(1) - 1) {
      // hours
      return TimeUnit.NANOSECONDS.toHours(ns) + "hours";
    } else {
      return "infinity";
    }
  }

  public long nanosMaxForBin(int index) {
    if (index < 0 || index >= bins.length) {
      throw new IllegalArgumentException();
    }
    if (index >= (bins.length - 1)) {
      return Long.MAX_VALUE;
    }
    long val = (1L << (index + smallestResolutionPower));
    return val;
  }

  public int getBinCount() {
    return binCount;
  }

  /**
   * Sloppy resets the LatencyBin. Note that counter and bin values may drift.
   */
  public void sloppyReset() {
    counter.reset();
    for (int i = 0; i < bins.length; i++) {
      bins[i].reset();
    }
  }

  public long nanosMinForBin(int index) {
    if (index < 0) {
      throw new IllegalArgumentException();
    }
    if (index == 0) {
      return 0L;
    }
    return nanosMaxForBin(index - 1) + 1;
  }

  /**
   * Create a Thread which loops at the specified interval, delaying after each one.
   * Note that this means a keepAlive which returns false and a repeatInterval of zero
   * will cause it to work as a one-shot.
   *
   * Will delay one interval before starting.
   *
   * @param keepAlive Boolean supplier which dictates if the loop should continue running.
   * @param delayInterval Delay interval for looping.
   * @param delayUnits Units for delay interval.
   * @param minNSToReport If no latencies &gt;= to this, no callback is made. &lt;=0 means always
   * report
   * @param output callback.
   * @return Thread, not started, set to daemon mode.
   */
  public Thread reporterThread(BooleanSupplier keepAlive,
                               long delayInterval,
                               TimeUnit delayUnits,
                               long minNSToReport,
                               Consumer<Log2LatencyBins> output) {
    Runnable runnable = reporterRunnable(keepAlive, delayInterval, delayUnits, minNSToReport, output);
    Thread ret = new Thread(() -> {
      try {
        delayUnits.sleep(delayInterval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      runnable.run();
    }, name + " latency watcher");
    ret.setDaemon(true);
    return ret;
  }

  /**
   * Create a runnable which loops at the specified interval, delaying after each one.
   * Note that this means a keepAlive which returns false and a repeatInterval of zero
   * will turn this into a single use (but reusable) Runnable.
   *
   * @param keepAlive Boolean supplier which dictates if the loop should continue running.
   * @param delayInterval Delay interval for looping.
   * @param delayUnits Units for delay interval.
   * @param minNSToReport If no latencies &gt;= to this, no callback is made. &lt;=0 means always
   * report
   * @param output callback.
   * @return Runnable
   */
  public Runnable reporterRunnable(BooleanSupplier keepAlive,
                                   long delayInterval,
                                   TimeUnit delayUnits,
                                   long minNSToReport,
                                   Consumer<Log2LatencyBins> output) {
    if (minNSToReport <= 0) {
      return () -> {
        do {
          output.accept(Log2LatencyBins.this);
          try {
            delayUnits.sleep(delayInterval);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        } while (keepAlive.getAsBoolean());
      };
    }

    final int minIndex = binIndexFor(minNSToReport);
    return () -> {
      do {
        long[] binCounts = binCounts();
        boolean found = false;
        for (int i = minIndex; !found && i < binCounts.length; i++) {
          if (binCounts[i] > 0) {
            found = true;
          }
        }
        if (found) {
          output.accept(Log2LatencyBins.this);
        }
        try {
          Thread.sleep(delayUnits.toMillis(delayInterval));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      } while (keepAlive.getAsBoolean());
    };

  }

  public String getName() {
    return name;
  }
}