/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.object;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.terracottatech.frs.object.CompleteKey;
import com.terracottatech.frs.object.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockObjectManager<I, K, V> implements ObjectManager<I, K, V> {

  private final Map<I, LinkedHashMap<K, Long>> map = new HashMap<I, LinkedHashMap<K, Long>>();

  private final Map<I, Map<K, V>> external;
  
  public MockObjectManager(Map<I, Map<K, V>> external) {
    this.external = external;
  }

  public long getLowestLsn() {
    Entry<CompleteKey<I, K>, Long> lowest = lowestEntry();
    if (lowest == null) {
      return -1;
    } else {
      return lowest.getValue();
    }
  }

  @Override
  public long getLsn(I id, K key) {
    LinkedHashMap<K, Long> m = map.get(id);
    if (m == null) {
      return -1;
    }
    Long lsn = m.get(key);
    return lsn == null ? -1 : lsn;
  }

  private Entry<CompleteKey<I, K>, Long> lowestEntry() {
    Entry<CompleteKey<I, K>, Long> lowest = null;
    for (Entry<I, LinkedHashMap<K, Long>> m : map.entrySet()) {
      Iterator<Entry<K, Long>> it = m.getValue().entrySet().iterator();
      if (it.hasNext()) {
        Entry<K, Long> e = it.next();
        if (lowest == null || e.getValue() < lowest.getValue()) {
          lowest = new SimpleEntry<CompleteKey<I, K>, Long>(new MockCompleteKey<I, K>(m.getKey(), e.getKey()), e.getValue());
        }
      }
    }
    return lowest;
  }
  
  public CompleteKey<I, K> getCompactionKey() {
    Entry<CompleteKey<I, K>, Long> lowest = lowestEntry();
    if (lowest == null) {
      return null;
    } else {
      return lowest.getKey();
    }
  }
  
  @Override
  public void put(I id, K key, V value, long lsn) {
    LinkedHashMap<K, Long> m = map.get(id);
    if (m == null) {
      m = new LinkedHashMap<K, Long>();
      map.put(id, m);
    }
    m.put(key, lsn);
  }

  @Override
  public void remove(I id, K key) {
    LinkedHashMap<K, Long> m = map.get(id);
    assert m != null;
    Long previous = m.remove(key);
    assert previous != null;
  }

  @Override
  public void delete(I id) {
    LinkedHashMap<K, Long> deleted = map.remove(id);
    assert deleted != null;
  }

  @Override
  public void replayPut(I id, K key, V value, long lsn) {
    Map<K, V> m = external.get(id);
    if (m == null) {
      m = new HashMap<K, V>();
      external.put(id, m);
    }
    m.put(key, value);
    put(id, key, value, lsn);
  }

  @Override
  public V replaceLsn(I id, K key, long newLsn) {
    LinkedHashMap<K, Long> m = map.get(id);
    if (m != null) {
      m.put(key, newLsn);
      Map<K, V> em = external.get(id);
      assert em != null;
      V value = em.get(key);
      assert value != null;
      return value;
    } else {
      return null;
    }
  }
}
