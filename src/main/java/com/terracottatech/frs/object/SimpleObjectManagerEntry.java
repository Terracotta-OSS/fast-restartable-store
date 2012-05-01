/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
