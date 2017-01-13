package com.terracottatech.frs.io.nio;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author cschanck
 **/
public class WritingSegmentJumpList implements Iterable<Long> {
  private final int chunkSize;
  private ArrayList<IntRun> buffers = new ArrayList<IntRun>();
  private long lastValue = Long.MIN_VALUE;
  private IntRun currentRun = null;
  private long firstValue = Long.MIN_VALUE;
  private int count = 0;
  private long byteFootprint = 0l;

  public WritingSegmentJumpList() {
    this(8 * 1024);
  }

  public WritingSegmentJumpList(int chunkSize) {
    this.chunkSize = chunkSize;
  }

  public void clear() {
    lastValue = Long.MIN_VALUE;
    firstValue = Long.MIN_VALUE;
    currentRun = null;
    count = 0;
    byteFootprint = 0l;
    buffers.clear();
  }

  public void add(long l) {
    if (buffers.isEmpty()) {
      lastValue = l;
      firstValue = l;
      currentRun = addNextBuffer();
      currentRun.addInt((int) (l - lastValue));
      count++;
    } else if (currentRun.isFull()) {
      currentRun = addNextBuffer();
      currentRun.addInt((int) (l - lastValue));
      lastValue = l;
      count++;
    } else {
      currentRun.addInt((int) (l - lastValue));
      lastValue = l;
      count++;
    }
  }

  private IntRun addNextBuffer() {
    IntRun run = new IntRun(new int[chunkSize]);
    buffers.add(run);
    byteFootprint = byteFootprint + chunkSize * 4;
    return run;
  }

  @Override
  public Iterator<Long> iterator() {
    return new Iterator<Long>() {
      int currIndex = 0;
      int runIndex = 0;
      long base = firstValue;

      public boolean hasNext() {
        return (currIndex < buffers.size() && runIndex < buffers.get(currIndex).getCount());
      }

      public Long next() {
        if (hasNext()) {
          IntRun run = buffers.get(currIndex);
          long ret = run.get(runIndex++) + base;
          // advance
          base = ret;
          if (runIndex >= run.getCount()) {
            currIndex++;
            runIndex = 0;
          }
          return ret;
        }
        throw new NoSuchElementException();
      }

      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public int size() {
    return count;
  }

  static class IntRun {
    private int[] iarr;
    private int count;

    public IntRun(int[] iarr) {
      this.iarr = iarr;
      count = 0;
    }

    public int getCapacity() {
      return iarr.length;
    }

    public boolean isFull() {
      return count >= iarr.length;
    }

    public int get(int index) {
      return iarr[index];
    }

    public int getCount() {
      return count;
    }

    public void addInt(int val) {
      iarr[count++] = val;
    }
  }
}
