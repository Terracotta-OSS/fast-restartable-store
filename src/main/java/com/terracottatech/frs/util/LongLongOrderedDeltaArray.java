package com.terracottatech.frs.util;

import java.util.ArrayList;

/**
 * This is a specialized class for adding ordered Long values in pairs,
 * where the distance between subsequent values (both key and value) is
 * no more than Integer.MAX_VALUE (within a chunk).
 * The keys must be added in increasing order.
 * <p>
 * This allows for useful compression and binary search lookups.
 * @author cschanck
 **/
public class LongLongOrderedDeltaArray {

  private final int chunkSize;
  private final int shiftAmount;
  private final int indexMask;
  private int count = 0;
  private ArrayList<LongLongRun> runs = new ArrayList<LongLongRun>();
  private int nextInsertionIndex = 0;
  private long lastKeyAdded = 0;

  public LongLongOrderedDeltaArray(int chunkSize) {
    int i = 1;
    while (i < chunkSize) {
      i = i << 1;
    }
    this.chunkSize = i;
    this.shiftAmount = Integer.numberOfTrailingZeros(i);
    this.indexMask = i - 1;
  }

  int getChunkSize() {
    return chunkSize;
  }

  int getShiftAmount() {
    return shiftAmount;
  }

  int getIndexMask() {
    return indexMask;
  }

  public int size() {
    return count;
  }

  public boolean isEmpty() {
    return count == 0;
  }

  public void clear() {
    runs.clear();
    lastKeyAdded = 0;
    nextInsertionIndex = 0;
    count = 0;
  }

  private LongLongRun runFor(int index) {
    return runs.get(runIndexFor(index));
  }

  private int runIndexFor(int index) {
    return index >>> shiftAmount;
  }

  private int indexFor(int index) {
    return index & indexMask;
  }

  public void append(long key, long value) {
    if (isEmpty()) {
      LongLongRun run = addRun();
      run.add(key, value);
      lastKeyAdded = key;
      nextInsertionIndex = 0;
      count = 1;
    } else {
      if (key <= lastKeyAdded) {
        throw new IllegalStateException("Markers added out of order; last: " + lastKeyAdded + " new: " + key);
      }
      LongLongRun run = runs.get(nextInsertionIndex);
      if (run.isFull()) {
        run = addRun();
        run.add(key, value);
        lastKeyAdded = key;
        nextInsertionIndex++;
        count++;
      } else {
        run.add(key, value);
        lastKeyAdded = key;
        count++;
      }
    }
  }

  private LongLongRun addRun() {
    LongLongRun ret = new LongLongRun(chunkSize);
    runs.add(ret);
    return ret;
  }

  public long getKey(int index) {
    return runFor(index).getKey(indexFor(index));
  }

  public long getValue(int index) {
    return runFor(index).getValue(indexFor(index));
  }

  public LongLongEntry get(int index) {
    return runFor(index).get(indexFor(index));
  }

  public void update(int index, long key, long value) {
    if (index == count) {
      append(key, value);
      return;
    }
    if (isEmpty()) {
      throw new IllegalStateException();
    }
    if (index > 0) {
      if (getKey(index - 1) >= key) {
        throw new IllegalArgumentException("Invalid ordering on set(): " + key);
      }
    }
    if ((index + 1) < count) {
      if (getKey(index + 1) <= key) {
        throw new IllegalArgumentException("Invalid ordering on set(): " + key);
      }
    }

    runFor(index).set(indexFor(index), key, value);
  }

  public int binarySearch(long target) {
    int min = 0;
    int max = size() - 1;
    while (max >= min) {
      int mid = min + (max - min) / 2;
      long cmp = target - getKey(mid);
      if (cmp == 0) {
        return mid;
      } else if (cmp > 0) {
        min = mid + 1;
      } else {
        max = mid - 1;
      }
    }
    return ~min;
  }

  static class LongLongRun {
    private final int max;
    private final int[] keys;
    private final int[] vals;
    private int count = 0;
    private long baseKey;
    private long baseValue;

    LongLongRun(int max) {
      this.max = max;
      this.keys=new int[max];
      this.vals=new int[max];
    }

    int size() {
      return count;
    }

    void add(long key, long value) {
      if (!isFull()) {

        if (count == 0) {
          baseKey = key;
          baseValue = value;
        }

        long kdiff = key - baseKey;
        if (kdiff > Integer.MAX_VALUE || kdiff < Integer.MIN_VALUE) {
          throw new IllegalStateException("Mark value span too large: " + baseKey + "/" + key);
        }
        long vdiff = value - baseValue;
        if (vdiff > Integer.MAX_VALUE || vdiff < Integer.MIN_VALUE) {
          throw new IllegalStateException("Start value span too large: " + baseValue + "/" + value);
        }

        keys[count]= (int) kdiff;
        vals[count]= (int) vdiff;
        count++;

      } else {
        throw new ArrayIndexOutOfBoundsException();
      }
    }

    long getKey(int index) {
      return baseKey + keys[index];
    }

    long getValue(int index) {
      return baseValue + vals[index];
    }

    LongLongEntry get(int index) {
      return new LongLongEntry(getKey(index), getValue(index));
    }

    boolean isFull() {
      return count == max;
    }

    void set(int index, long key, long value) {

      long kdiff = key - baseKey;
      if (kdiff > Integer.MAX_VALUE || kdiff < Integer.MIN_VALUE) {
        throw new IllegalStateException("Key value span too large: " + baseKey + "/" + key);
      }
      long vdiff = value - baseValue;
      if (vdiff > Integer.MAX_VALUE || vdiff < Integer.MIN_VALUE) {
        throw new IllegalStateException("Value value span too large: " + baseValue + "/" + value);
      }
      keys[index]= (int) kdiff;
      vals[index]= (int) vdiff;
    }
  }

  public static class LongLongEntry {
    private final long key;
    private final long value;

    public LongLongEntry(long key, long value) {
      this.key = key;
      this.value = value;
    }

    public long getKey() {
      return key;
    }

    public long getValue() {
      return value;
    }
  }
}


