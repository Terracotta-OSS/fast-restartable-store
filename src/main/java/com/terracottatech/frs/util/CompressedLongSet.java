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

import java.util.*;

/**
 * @author tim
 */
public class CompressedLongSet extends AbstractSet<Long> {
  protected transient volatile int modCount;
  protected final AATreeSet<BitSet> ranges = new AATreeSet<BitSet>();
  protected       int               size   = 0;

  public CompressedLongSet() {
    super();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    this.size = 0;
    this.modCount++;
    this.ranges.clear();
  }

  @Override
  public boolean addAll(Collection<? extends Long> c) {
    if (c instanceof CompressedLongSet) {
      CompressedLongSet other = (CompressedLongSet) c;
      /**
       * Optimized addAll method if the other collection is a BitSetObjectIDSet too.<br>
       * <p>
       * Some Assumptions are <br>
       * 1. AATreeSetIterator iterates in sorted order <br>
       * 2. start is always in fixed multiple
       */
      final int oldSize = size;
      final Iterator<BitSet> myRanges = ranges.iterator();
      final Iterator<BitSet> otherRanges = other.ranges.iterator();
      final List<BitSet> toAdd = new ArrayList<BitSet>();
      BitSet currentMine = null;
      while (otherRanges.hasNext()) {
        if (currentMine == null) {
          // First Iteration
          if (myRanges.hasNext()) {
            currentMine = myRanges.next();
          } else {
            // No ranges in this set, just clone and add and return
            copyAndAddAll(otherRanges);
            break;
          }
        }
        final BitSet nextOther = otherRanges.next();
        while (currentMine.start < nextOther.start && myRanges.hasNext()) {
          currentMine = myRanges.next();
        }
        if (currentMine.start == nextOther.start) {
          // Same range, can be merged
          final long sizeBefore = currentMine.size();
          currentMine.addAll(nextOther);
          this.size += currentMine.size() - sizeBefore;
        } else {
          // currentMine.start > nextOther.start || !myRanges.hasNext()
          toAdd.add(nextOther);
          if (currentMine.start < nextOther.start && !myRanges.hasNext()) {
            // No more ranges in this set, copy the rest directly saving a copy
            copyAndAddAll(otherRanges);
          }
        }
      }
      copyAndAddAll(toAdd.iterator());
      return (oldSize < this.size);
    } else {
      return super.addAll(c);
    }
  }

  private void copyAndAddAll(final Iterator<BitSet> i) {
    for (; i.hasNext();) {
      final BitSet copied = new BitSet(i.next());
      this.size += copied.size();
      boolean added = this.ranges.add(copied);
      if (!added) { throw new AssertionError("cloned : " + copied + " is not added to this set : " + this); }
    }
  }

  @Override
  public boolean add(Long l) {
    if (l == null) throw new NullPointerException();

    // need to handle -ve ids
    final long start = calculateStart(l);
    int nextRangeMaskBit;
    if (l < 0) {
      nextRangeMaskBit = (int) (BitSet.RANGE_SIZE - ((-l) % BitSet.RANGE_SIZE));
    } else {
      nextRangeMaskBit = (int) (l % BitSet.RANGE_SIZE);
    }

    // Step 1 : Check if number can be contained in any of the range, if so add to the same Range.
    final BitSet prev = ranges.find(new BitSet(start, 0));
    if (prev != null) {
      final boolean isAdded = prev.add(l);
      if (isAdded) {
        this.size++;
        this.modCount++;
      }
      return isAdded;
    }

    // Step 2: Add a new range for just this number.
    final long nextRange = 1L << nextRangeMaskBit;
    final BitSet newRange = new BitSet(start, nextRange);
    final boolean isAdded = this.ranges.add(newRange);
    if (isAdded) {
      this.size++;
      this.modCount++;
    }
    return isAdded;
  }

  @Override
  public boolean remove(Object o) {
    if (o instanceof Long) {
      final long l = (Long) o;

      final long start = calculateStart(l);

      final BitSet current = this.ranges.find(new BitSet(start, 0));
      if (current == null) {
        // Not found
        return false;
      }
      if (current.remove(l)) {
        if (current.isEmpty()) {
          ranges.remove(current);
        }
        size--;
        modCount++;
        return true;
      }
      return false;
    } else {
      return false;
    }
  }

  @Override
  public boolean contains(final Object o) {
    if (o instanceof Long) {
      long l = (Long) o;
      final long start = calculateStart(l);
      final BitSet r = ranges.find(new BitSet(start, 0));
      return r != null && isPresent(l, r);
    } else {
      return false;
    }
  }

  private boolean isPresent(final long lid, final BitSet r) {
    final long maskBit = 1L << (int) (lid - r.start);
    return (r.nextLongs & maskBit) != 0;
  }

  @Override
  public Iterator<Long> iterator() {
    return new CompressedLongSetIterator();
  }

  private static long calculateStart(final long lid) {
    if (lid < 0) {
      return (-BitSet.RANGE_SIZE + ((lid + 1) / BitSet.RANGE_SIZE) * BitSet.RANGE_SIZE);
    } else {
      return (lid - (lid % BitSet.RANGE_SIZE));
    }
  }

  private class CompressedLongSetIterator implements Iterator<Long> {
    private Iterator<BitSet> nodes;
    private BitSet   current;
    private BitSet   next;
    private Long     lastReturned;
    private int      idx;
    private int      expectedModCount;

    CompressedLongSetIterator() {
      this.nodes = ranges.iterator();
      this.expectedModCount = modCount;
      this.idx = 0;
      if (this.nodes.hasNext()) {
        this.current = this.nodes.next();
      }
      this.next = (this.nodes.hasNext() ? this.nodes.next() : null);
    }

    public boolean hasNext() {
      return (this.next != null) || (this.current != null && !isPointingToLast());
    }

    private boolean isPointingToLast() {
      return current.last() < current.start + idx;
    }

    public Long next() {
      if (this.current == null) { throw new NoSuchElementException(); }
      if (this.expectedModCount != modCount) { throw new ConcurrentModificationException(); }
      moveToNextIndex();
      final long l = this.current.start + this.idx;
      this.idx++;
      return (this.lastReturned = l);
    }

    private void moveToNextIndex() {
      if (this.current == null) {
        this.idx = 0;
        return;
      }

      long maskBit = 1L << this.idx;
      while (((this.current.nextLongs & maskBit) == 0) && this.idx < BitSet.RANGE_SIZE) {
        this.idx++;
        maskBit = 1L << this.idx;
      }
      if (this.idx >= BitSet.RANGE_SIZE) {
        moveToNextGroup();
      }
    }

    private void moveToNextGroup() {
      this.idx = 0;
      if (this.next != null) {
        this.current = this.next;
        moveToNextIndex();
        this.next = this.nodes.hasNext() ? this.nodes.next() : null;
      } else {
        this.current = null;
      }
    }

    public void remove() {
      if (this.lastReturned == null) { throw new IllegalStateException(); }
      if (this.expectedModCount != modCount) { throw new ConcurrentModificationException(); }

      final long l = this.lastReturned;
      final long lastElement = this.current.last();
      this.current.remove(lastReturned);
      size--;
      modCount++;

      if (!this.current.isEmpty()) {
        if (lastElement == this.lastReturned) {
          // if it was the highest element in the range set then move the pointer to next
          this.current = this.next;
          this.next = this.nodes.hasNext() ? this.nodes.next() : null;
          this.idx = 0;
        }
      } else {
        // if all the elements got removed because of this removal then remove the node
        // and create tailset iterator
        ranges.remove(this.current);
        this.nodes = ranges.tailSet(new BitSet(calculateStart(l), 0)).iterator();
        this.idx = 0;
        this.current = this.nodes.hasNext() ? this.nodes.next() : null;
        this.next = this.nodes.hasNext() ? this.nodes.next() : null;
      }

      if (this.current != null) {
        moveToNextIndex();
      } else {
        this.idx = 0;
      }

      this.expectedModCount = modCount;
      this.lastReturned = null;
    }
  }

  private static class BitSet extends AATreeSet.AbstractTreeNode<BitSet> implements Comparable<BitSet> {
    private long            start;
    private long            nextLongs  = 0;
    public static final int RANGE_SIZE = 64;

    public BitSet(final long start, final long nextRanges) {
      this.start = start;
      this.nextLongs = nextRanges;
    }

    public BitSet(BitSet copyThis) {
      this(copyThis.start, copyThis.nextLongs);
    }

    public void addAll(final BitSet other) {
      if (this.start != other.start) { throw new AssertionError("Ranges : Start is not the same. mine : " + this.start
                                                                        + " other : " + other.start); }
      this.nextLongs |= other.nextLongs;
    }

    @Override
    public String toString() {
      return "Range(" + this.start + "," + Long.toBinaryString(this.nextLongs) + ")";
    }

    public boolean isEmpty() {
      return this.nextLongs == 0;
    }

    public long size() {
      return (Long.bitCount(this.nextLongs)); // since it is all inclusive
    }

    public boolean remove(final long lid) {
      if (lid < this.start || lid >= this.start + RANGE_SIZE) { throw new AssertionError(
              "Ranges : Illegal value passed to remove : "
                      + this
                      + " remove called for : "
                      + lid); }
      long maskBits = 1L << (int) (lid - this.start);
      maskBits &= this.nextLongs;
      this.nextLongs ^= maskBits;
      return (maskBits != 0);
    }

    public boolean add(final long lid) {
      if (lid < this.start || lid >= this.start + RANGE_SIZE) { throw new AssertionError(
              "Ranges : Illegal value passed to add : "
                      + this
                      + " add called for : "
                      + lid); }
      final long maskBits = 1L << (int) (lid - this.start);
      if ((this.nextLongs & maskBits) == 0) {
        this.nextLongs = this.nextLongs | maskBits;
        return true;
      }
      return false;
    }

    /**
     * while comparing we only care about start since that tells us the starting point of the sets of integer in this
     * bit set
     */
    public int compareTo(final BitSet o) {
      if (this.start < o.start) {
        return -1;
      } else if (this.start == o.start) {
        return 0;
      } else {
        return 1;
      }
    }

    /**
     * this returns true if start and nextLongs both are equal, Note that compareTo does not hold the same contract
     */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BitSet)) return false;
      BitSet o = (BitSet) obj;
      return (this.start == o.start) && (this.nextLongs == o.nextLongs);
    }

    @Override
    public int hashCode() {
      return (int) (this.start * 31 + nextLongs);
    }

    public void swapPayload(final AATreeSet.Node<BitSet> other) {
      if (other instanceof BitSet) {
        final BitSet r = (BitSet) other;
        long temp = this.start;
        this.start = r.start;
        r.start = temp;
        temp = this.nextLongs;
        this.nextLongs = r.nextLongs;
        r.nextLongs = temp;
      } else {
        throw new AssertionError("AATree can't contain both Ranges and other types : " + this + " other : " + other);
      }
    }

    public BitSet getPayload() {
      return this;
    }

    public long first() {
      if (this.nextLongs == 0) { throw new NoSuchElementException(); }
      return this.start + Long.numberOfTrailingZeros(this.nextLongs);
    }

    public long last() {
      if (this.nextLongs == 0) { throw new NoSuchElementException(); }
      return this.start + BitSet.RANGE_SIZE - 1 - Long.numberOfLeadingZeros(this.nextLongs);
    }
  }
}
