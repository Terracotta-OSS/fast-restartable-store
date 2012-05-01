/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Set;

public abstract class AbstractObjectManagerStripe<I, K, V> implements ObjectManagerStripe<I, K, V> {
  
  @Override
  public Long getLowestLsn() {
    long lowest = -1;
    for (ObjectManagerSegment<I, K, V> segment : getSegments()) {
      Long firstLsn = segment.getLowestLsn();
      if (firstLsn != null) {
        if (lowest < 0 || firstLsn < lowest) {
          lowest = firstLsn;
        }
      }
    }
    return lowest;
  }

  @Override
  public Long getLsn(K key) {
    int hash = extractHashCode(key);
    return getSegmentFor(hash, key).getLsn(hash, key);
  }

  @Override
  public void put(K key, V value, long lsn) {
    int hash = extractHashCode(key);
    getSegmentFor(hash, key).put(hash, key, value, lsn);
  }

  @Override
  public void remove(K key) {
    int hash = extractHashCode(key);
    getSegmentFor(hash, key).remove(hash, key);
  }

  @Override
  public Set<Long> replayPut(K key, V value, long lsn) {
    int hash = extractHashCode(key);
    return getSegmentFor(hash, key).replayPut(hash, key, value, lsn);
  }

  @Override
  public void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn) {
    int hash = extractHashCode(entry.getKey());
    getSegmentFor(hash, entry.getKey()).updateLsn(hash, entry, newLsn);
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry) {
    int hash = extractHashCode(entry.getKey());
    getSegmentFor(hash, entry.getKey()).releaseCompactionEntry(entry);
  }

  public long size() {
    long total = 0;
    for (ObjectManagerSegment<?, ?, ?> s : getSegments()) {
      total += s.size();
    }
    return total;
  }
  
  protected abstract ObjectManagerSegment<I, K, V> getSegmentFor(int hash, K key);
  
  protected abstract int extractHashCode(K key);
}
