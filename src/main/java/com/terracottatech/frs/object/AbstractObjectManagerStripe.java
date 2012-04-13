/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

public abstract class AbstractObjectManagerStripe<I, K, V> implements ObjectManagerStripe<I, K, V> {
  
  @Override
  public Long getLowestLsn() {
    long lowest = -1;
    for (ObjectManagerSegment<I, K, V> segment : getSegments()) {
      Long firstLsn = segment.firstLsn();
      if (firstLsn != null) {
        if (lowest < 0 || firstLsn < lowest) {
          lowest = firstLsn;
        }
      }
    }
    return lowest;
  }

  @Override
  public long getLsn(K key) {
    Long l = getSegmentFor(key).getLsn(key);
    if ( l == null ) return -1;
    return l;
  }

  @Override
  public void put(K key, V value, long lsn) {
    getSegmentFor(key).put(key, value, lsn);
  }

  @Override
  public void remove(K key) {
    getSegmentFor(key).remove(key);
  }

  @Override
  public void replayPut(K key, V value, long lsn) {
    getSegmentFor(key).replayPut(key, value, lsn);
  }

  @Override
  public V replaceLsn(K key, long newLsn) {
    return getSegmentFor(key).replaceLsn(key, newLsn);
  }

  protected abstract ObjectManagerSegment<I, K, V> getSegmentFor(K key);
}
