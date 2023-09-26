/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Chris Dennis
 */
public abstract class AbstractObjectManager<I, K, V> implements ObjectManager<I, K, V> {

  private final ConcurrentLinkedQueue<ObjectManagerSegment<I, K, V>> compactionTargets = new ConcurrentLinkedQueue<ObjectManagerSegment<I, K, V>>();
  
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
    getStripeFor(id).delete();
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
    boolean refreshed = false;
    while (true) {
      ObjectManagerSegment<I, K, V> stripe = compactionTargets.poll();
      if (stripe == null) {
        if (!refreshed) {
          refreshCompactionTargets();
          refreshed = true;
          continue;
        } else {
          return null;
        }
      }
      ObjectManagerEntry<I, K, V> entry = stripe.acquireCompactionEntry(ceilingLsn);
      if (entry != null) {
        return entry;
      }
    }
  }

  private void refreshCompactionTargets() {
    if (compactionTargets.isEmpty()) {
      for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
        compactionTargets.addAll(stripe.getSegments());
      }
    }
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry) {
    getStripeFor(entry.getId()).releaseCompactionEntry(entry);
  }

    @Override
  public long getLowestLsn() {
    long lowest = -1;
    for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
      Long lowestInStripe = stripe.getLowestLsn();
      if (lowestInStripe != null) {
        if (lowest < 0 || lowestInStripe < lowest) {
          lowest = lowestInStripe;
        }
      }
    }
    return lowest;
  }

  public long size() {
    long size = 0;
    for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
      size += stripe.size();
    }
    return size;
  }

  @Override
  public long sizeInBytes() {
    long size = 0;
    for (ObjectManagerStripe<I, K, V> stripe : getStripes()) {
      size += stripe.sizeInBytes();
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

  protected abstract Collection<ObjectManagerStripe<I, K, V>> getStripes();
}
