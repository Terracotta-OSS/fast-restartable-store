/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Chris Dennis
 */
abstract class AbstractObjectManager<I, K, V> implements ObjectManager<I, K, V> {

  private final Object compactionMutex = new Object();
  private final ConcurrentLinkedQueue<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> compactionTargets = new ConcurrentLinkedQueue<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>();
  
  private volatile long latestLowestLsn = -1;

  @Override
  public long getLowestLsn() {
    return latestLowestLsn;
  }

  @Override
  public long getLsn(I id, K key) {
    return getStripeFor(id, key).get(key).getLsn();
  }

  @Override
  public void put(I id, K key, V value, long lsn) {
    getStripeFor(id, key).put(key, new SequencedValue(value, lsn));
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
    getStripeFor(id, key).put(key, new SequencedValue<V>(value, lsn));
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
    ConcurrentValueSortedMap<I, K, SequencedValue<V>> stripe = getCompactionSource();
    if (stripe == null) {
      return null;
    } else {
      Entry<K, SequencedValue<V>> first = stripe.firstEntry();
      if (first == null) {
        return null;
      } else {
        return new SimpleCompleteKey<I, K>(stripe.identifier(), first.getKey());
      }
    }
  }

  private ConcurrentValueSortedMap<I, K, SequencedValue<V>> getCompactionSource() {
    ConcurrentValueSortedMap<I, K, SequencedValue<V>> source = compactionTargets.poll();
    if (source == null) {
      synchronized (compactionMutex) {
        if (compactionTargets.isEmpty()) {
          for (List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> map : getStripes()) {
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
    ConcurrentValueSortedMap<I, K, SequencedValue<V>> stripe = getStripeFor(id, key);
    SequencedValue<V> seqValue = stripe.get(key);
    if (!stripe.replace(key, seqValue, new SequencedValue<V>(seqValue.getValue(), newLsn))) {
      throw new AssertionError();
    }
    return seqValue.getValue();
  }
  
  public void updateLowestLsn() {
    long lowest = -1;
    for (List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> stripes : getStripes()) {
      for (ConcurrentValueSortedMap<I, K, SequencedValue<V>> stripe : stripes) {
        Entry<K, SequencedValue<V>> firstEntry = stripe.firstEntry();
        if (firstEntry != null) {
          long stripeLowest = firstEntry.getValue().getLsn();
          if (lowest < 0 || stripeLowest < lowest) {
            lowest = stripeLowest;
          }
          lowest = Math.min(lowest, stripeLowest);
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
  protected abstract ConcurrentValueSortedMap<I, K, SequencedValue<V>> getStripeFor(I id, K key);

  protected abstract void deleteStripesFor(I id);
  
  protected abstract Collection<List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>> getStripes();
}
