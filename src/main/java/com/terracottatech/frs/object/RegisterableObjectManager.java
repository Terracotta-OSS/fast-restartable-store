/*
 * Copyright (c) 2012-2024 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Aggregated ObjectManager that supports registering stripes.
 *
 * @author tim
 */
public class RegisterableObjectManager<I, K, V> extends AbstractObjectManager<I, K, V> {
  private final ConcurrentMap<I, ObjectManagerStripe<I, K, V>> stripes = new ConcurrentHashMap<>();
  private final ConcurrentMap<I, Entry<Integer, Integer>> tokenRangeMap = new ConcurrentHashMap<>();
  private final AtomicInteger currentToken = new AtomicInteger(0);

  @Override
  protected ObjectManagerStripe<I, K, V> getStripeFor(I id) {
    return stripes.get(id);
  }

  @Override
  public int replayConcurrency(I id, K key) {
    ObjectManagerStripe<I, K, V> stripe = stripes.get(id);
    int concurrency = stripe.replayConcurrency(key);
    if (concurrency == -1) {
      return stripe.hashCode();
    } else {
      Entry<Integer, Integer> tokenRange = tokenRangeMap.get(id);
      concurrency = concurrency & (tokenRange.getValue() - tokenRange.getKey());
      return tokenRange.getKey() + concurrency;
    }
  }

  @Override
  protected Collection<ObjectManagerStripe<I, K, V>> getStripes() {
    return stripes.values();
  }

  public void registerObject(RestartableObject<I, K, V> object) {
    registerObject(object, 8);
  }

  public void registerObject(RestartableObject<I, K, V> object, int shards) {
    registerStripe(object.getId(), object.getObjectManagerStripe());

    // convert the value to a power of two
    int shardRange = Integer.highestOneBit(shards);
    if (shardRange != shards) {
      shardRange = shardRange << 1;
    }
    // token range [start, end] both inclusive
    int rangeStart = currentToken.getAndAdd(shardRange);
    int rangeEnd = rangeStart + shardRange - 1;
    tokenRangeMap.put(object.getId(), new SimpleEntry<>(rangeStart, rangeEnd));
  }

  public void registerStripe(I id, ObjectManagerStripe<I, K, V> stripe) {
    ObjectManagerStripe<?, ?, ?> previous = stripes.putIfAbsent(id, stripe);
    if (previous != null) {
      throw new IllegalStateException(id + " already mapped");
    }
  }

  public void unregisterStripe(I id) {
    if (stripes.remove(id) == null) {
      throw new AssertionError(id + " not mapped");
    }
  }
}
