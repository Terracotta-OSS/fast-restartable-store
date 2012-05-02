/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Collection;
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
    Long l = getStripeFor(id).getLsn(key);
    return l == null ? -1 : l;
  }

  @Override
  public void put(I id, K key, V value, long lsn) {
    getStripeFor(id).put(key, value, lsn);
  }

  @Override
  public void delete(I id) {
    deleteStripeFor(id);
  }

  @Override
  public void remove(I id, K key) {
    getStripeFor(id).remove(key);
  }

  @Override
  public void replayPut(I id, K key, V value, long lsn) {
    getStripeFor(id).replayPut(key, value, lsn);
  }

  @Override
  public void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn) {
    getStripeFor(entry.getId()).updateLsn(entry, newLsn);
  }

  /**
   * For initial implementations as long as this returns a key biased toward the
   * early records (and eventually returns <em>all</em> early records) then that
   * is good enough.
   * 
   * @return Compaction entry
   */
  @Override
  public ObjectManagerEntry<I, K, V> acquireCompactionEntry(long ceilingLsn) {
    ObjectManagerSegment<I, K, V> stripe = getCompactionSource();
    if (stripe == null) {
      return null;
    } else {
      return stripe.acquireCompactionEntry(ceilingLsn);
    }
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry) {
    getStripeFor(entry.getId()).releaseCompactionEntry(entry);
  }

  private ObjectManagerSegment<I, K, V> getCompactionSource() {
    ObjectManagerSegment<I, K, V> source = compactionTargets.poll();
    if (source == null) {
      synchronized (compactionMutex) {
        if (compactionTargets.isEmpty()) {
          for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
            compactionTargets.addAll(stripe.getSegments());
          }
        }
        return compactionTargets.poll();
      }
    } else {
      return source;
    }
  }
  
  public void updateLowestLsn() {
    long lowest = -1;
    for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
      Long lowestInStripe = stripe.getLowestLsn();
      if (lowestInStripe != null) {
	    if (lowest < 0 || lowestInStripe < lowest) {
	      lowest = lowestInStripe;
	    }
	  }
    }
    latestLowestLsn = lowest;
  }

  public long size() {
    long size = 0;
    for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
      size += stripe.size();
    }
    return size;
  }
  
  /**
   * Returns the stripes of a segmented sorted map.
   * <p>
   * Each stripe in the {@code StripeSortedMap} is sorted by LSN.  Finding the
   * true minimum requires locking all stripes and finding the minimal tail.  A
   * valid historical value can be retrieved without locking all the stripes
   * concurrently.
   */
  protected abstract ObjectManagerStripe<I, K, V> getStripeFor(I id);

  protected abstract void deleteStripeFor(I id);
  
  protected abstract Collection<ObjectManagerStripe<I, K, V>> getStripes();
}
