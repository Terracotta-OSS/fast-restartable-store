/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractObjectManager<I, K, V> implements ObjectManager<I, K, V> {

  private final Object compactionMutex = new Object();
  private final ConcurrentLinkedQueue<ObjectManagerSegment<I, K, V>> compactionTargets = new ConcurrentLinkedQueue<ObjectManagerSegment<I, K, V>>();
  
  private volatile long latestLowestLsn = -1;

  @Override
  public long getLowestLsn() {
    return latestLowestLsn;
  }

  @Override
  public long getLsn(I id, K key) {
    return getStripeFor(id, key).getLsn(key);
  }

  @Override
  public void put(I id, K key, V value, long lsn) {
    getStripeFor(id, key).put(key, value, lsn);
  }

  @Override
  public void delete(I id) {
    deleteStripesFor(id);
  }

  @Override
  public void remove(I id, K key) {
    getStripeFor(id, key).remove(key);
  }

  @Override
  public void replayPut(I id, K key, V value, long lsn) {
    getStripeFor(id, key).replayPut(key, value, lsn);
  }

  /**
   * For initial implementations as long as this returns a key biased toward the
   * early records (and eventually returns <em>all</em> early records) then that
   * is good enough.
   * 
   * @return 
   */
  @Override
  public CompleteKey<I, K> getCompactionKey() {
    ObjectManagerSegment<I, K, V> stripe = getCompactionSource();
    if (stripe == null) {
      return null;
    } else {
      K firstKey = stripe.firstKey();
      if (firstKey == null) {
        return null;
      } else {
        return new SimpleCompleteKey<I, K>(stripe.identifier(), firstKey);
      }
    }
  }

  private ObjectManagerSegment<I, K, V> getCompactionSource() {
    ObjectManagerSegment<I, K, V> source = compactionTargets.poll();
    if (source == null) {
      synchronized (compactionMutex) {
        if (compactionTargets.isEmpty()) {
          for (List<ObjectManagerSegment<I, K, V>> map : getStripes()) {
            compactionTargets.addAll(map);
          }
        }
        return compactionTargets.poll();
      }
    } else {
      return source;
    }
  }
  
  @Override
  public V replaceLsn(I id, K key, long newLsn) {
    ObjectManagerSegment<I, K, V> stripe = getStripeFor(id, key);
    V value = stripe.get(key);
    if (!stripe.replaceLsn(key, value, newLsn)) {
      throw new AssertionError();
    }
    return value;
  }
  
  public void updateLowestLsn() {
    long lowest = -1;
    for (List<ObjectManagerSegment<I, K, V>> stripes : getStripes()) {
      for (ObjectManagerSegment<I, K, V> stripe : stripes) {
        Long firstLsn = stripe.firstLsn();
        if (firstLsn != null) {
          if (lowest < 0 || firstLsn < lowest) {
            lowest = firstLsn;
          }
        }
      }
    }
    latestLowestLsn = lowest;
  }
  
  /**
   * Returns the stripes of a segmented sorted map.
   * <p>
   * Each stripe in the {@code StripeSortedMap} is sorted by LSN.  Finding the
   * true minimum requires locking all stripes and finding the minimal tail.  A
   * valid historical value can be retrieved without locking all the stripes
   * concurrently.
   */
  protected abstract ObjectManagerSegment<I, K, V> getStripeFor(I id, K key);

  protected abstract void deleteStripesFor(I id);
  
  protected abstract Collection<List<ObjectManagerSegment<I, K, V>>> getStripes();
}
