/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.io.Serializable;

/**
 *
 * @author Chris Dennis
 */
public final class SequencedValue<V> implements Serializable, Comparable<SequencedValue<V>> {
  private final V value;
  private final long lsn;

  public SequencedValue(V value, long lsn) {
    this.value = value;
    this.lsn = lsn;
  }

  public long getLsn() {
    return lsn;
  }

  public V getValue() {
    return value;
  }

  @Override
  public int compareTo(SequencedValue<V> t) {
    long diff = getLsn() - t.getLsn();
    if (diff < 0) {
      return -1;
    } else if (diff > 0) {
      return 1;
    } else {
      return 0;
    }
  }
}
