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

import java.io.Serializable;

/**
 * @author tim
 */
public class SimpleObjectManagerEntry<I, K, V> implements ObjectManagerEntry<I, K, V>,
        Serializable {

  private final I id;
  private final K key;
  private final V value;
  private final long lsn;

  public SimpleObjectManagerEntry(I id, K key, V value, long lsn) {
    this.id = id;
    this.key = key;
    this.value = value;
    this.lsn = lsn;
  }

  public I getId() {
    return id;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  public long getLsn() {
    return lsn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SimpleObjectManagerEntry that = (SimpleObjectManagerEntry) o;

    return lsn == that.lsn && id.equals(that.id) && key.equals(that.key) && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + (int) (lsn ^ (lsn >>> 32));
    return result;
  }
}
