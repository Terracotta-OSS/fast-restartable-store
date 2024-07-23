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
package com.terracottatech.frs.object;

import com.terracottatech.frs.Constants;

/**
 * @author tim
 */
public class NullObjectManager<I, K, V> implements ObjectManager<I, K, V> {


  @Override
  public long getLsn(I id, K key) {
    return 0;
  }

  @Override
  public void put(I id, K key, V value, long lsn) {
  }

  @Override
  public void delete(I id) {
  }

  @Override
  public void remove(I id, K key) {
  }

  @Override
  public void replayPut(I id, K key, V value, long lsn) {
  }

  @Override
  public ObjectManagerEntry<I, K, V> acquireCompactionEntry(long ceilingLsn) {
    return null;
  }

  @Override
  public void releaseCompactionEntry(ObjectManagerEntry<I, K, V> ikvObjectManagerEntry) {
  }

  @Override
  public void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn) {
  }

  @Override
  public long size() {
    return 0;
  }

  @Override
  public long getLowestLsn() {
      return Constants.GENESIS_LSN;
  }

  @Override
  public long sizeInBytes() {
    return 0;
  }
}
