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

import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractObjectManagerStripe<I, K, V> implements ObjectManagerStripe<I, K, V> {
  private final ConcurrentHashMap<K, Integer> extractedHashCodes = new ConcurrentHashMap<>();
  
  @Override
  public Long getLowestLsn() {
    Long lowest = null;
    for (ObjectManagerSegment<I, K, V> segment : getSegments()) {
      Long firstLsn = segment.getLowestLsn();
      if (firstLsn != null) {
        if (lowest == null || firstLsn < lowest) {
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
  public void replayPut(K key, V value, long lsn) {
    Integer hash = extractedHashCodes.remove(key);
    if (hash == null) {
      hash = extractHashCode(key);
    }
    getSegmentFor(hash, key).replayPut(hash, key, value, lsn);
  }

  @Override
  public int replayConcurrency(K key) {
    int hash = extractHashCode(key);
    extractedHashCodes.put(key, hash);
    return getSegmentFor(hash, key).hashCode();
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

  @Override
  public long sizeInBytes() {
    long size = 0;
    for (ObjectManagerSegment<I, K, V> segment : getSegments()) {
      size += segment.sizeInBytes();
    }
    return size;
  }

  protected abstract ObjectManagerSegment<I, K, V> getSegmentFor(int hash, K key);
  
  protected abstract int extractHashCode(K key);
}
