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

import java.util.Collection;

public interface ObjectManagerStripe<I, K, V> {

  Long getLowestLsn();

  Long getLsn(K key);

  void put(K key, V value, long lsn);

  void remove(K key);

  void delete();
  
  void replayPut(K key, V value, long lsn);

  default int replayConcurrency(K key) {
    return -1;
  }

  Collection<ObjectManagerSegment<I, K, V>> getSegments();

  void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn);

  void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry);

  long size();

  long sizeInBytes();
}
