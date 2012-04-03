/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object.heap;

import com.terracottatech.frs.object.AbstractObjectManager;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ValueSortedMap;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author Chris Dennis
 */
class HeapObjectManager<I, K, V> extends AbstractObjectManager<I, K, V> {

  private final ConcurrentMap<I, List<ObjectManagerSegment<I, K, V>>> maps = new ConcurrentHashMap<I, List<ObjectManagerSegment<I, K, V>>>();
  private final int concurrency;
  
  public HeapObjectManager(int concurrency) {
    this.concurrency = concurrency;
  }
  
  
  @Override
  protected ObjectManagerSegment<I, K, V> getStripeFor(I id, K key) {
    int spreadHash = spread(key.hashCode());
    List<ObjectManagerSegment<I, K, V>> map = maps.get(id);
    if (map == null) {
      map = createStripes(id);
      List<ObjectManagerSegment<I, K, V>> racer = maps.putIfAbsent(id, map);
      if (racer != null) {
        map = racer;
      }
    }
    return map.get(Math.abs(spreadHash % map.size()));
  }

  @Override
  protected void deleteStripesFor(I id) {
    maps.remove(id);
  }

  @Override
  protected Collection<List<ObjectManagerSegment<I, K, V>>> getStripes() {
    return maps.values();
  }
  
  private int spread(int hash) {
    return hash;
  }

  private List<ObjectManagerSegment<I, K, V>> createStripes(I identifier) {
    List<ObjectManagerSegment<I, K, V>> stripes = new ArrayList<ObjectManagerSegment<I, K, V>>(concurrency);
    for (int i = 0; i < concurrency; i++) {
      stripes.add(new InHeapObjectManagerSegment<I, K, V>(identifier));
    }
    return stripes;
  }
  
  /*
   * Ignoring thread-scheduling jitter values should arrive at the map approximately
   * ordered.  We should be able to take advantage of that in the collection
   * implementation.
   * 
   * @param <I>
   * @param <K>
   * @param <V> 
   */
  static class InHeapObjectManagerSegment<I, K, V> implements ObjectManagerSegment<I, K, V> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final I identifier;
    
    private final Map<K, V> dataMap = new HashMap<K, V>();
    private final ValueSortedMap<K, Long> lsnMap = new HeapValueSortedMap();
    
    public InHeapObjectManagerSegment(I identifier) {
      this.identifier = identifier;
    }
    
    @Override
    public I identifier() {
      return identifier;
    }

    @Override
    public K firstKey() {
      Lock l = lock.readLock();
      l.lock();
      try {
        K firstKey = lsnMap.firstKey();
        if (firstKey == null) {
          assert dataMap.size() == lsnMap.size();
          return null;
        } else {
          assert dataMap.size() == lsnMap.size();
          return firstKey;
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public Long firstLsn() {
      Lock l = lock.readLock();
      l.lock();
      try {
        Long firstLsn = lsnMap.firstValue();
        if (firstLsn == null) {
          assert dataMap.size() == lsnMap.size();
          return null;
        } else {
          assert dataMap.size() == lsnMap.size();
          return firstLsn;
        }
      } finally {
        l.unlock();
      }
    }
    
    @Override
    public V get(K key) {
      Lock l = lock.readLock();
      l.lock();
      try {
        return dataMap.get(key);
      } finally {
        l.unlock();
      }
    }

    @Override
    public Long getLsn(K key) {
      Lock l = lock.readLock();
      l.lock();
      try {
        return lsnMap.get(key);
      } finally {
        l.unlock();
      }
    }
    
    @Override
    public void replayPut(K key, V value, long lsn) {
      put(key, value, lsn);
    }

    @Override
    public void put(K key, V value, long lsn) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        Node<K, V> node = new Node<K, V>(key, value, lsn);
        dataMap.put(key, value);
        lsnMap.put(key, lsn);
        assert dataMap.size() == lsnMap.size();
      } finally {
        l.unlock();
      }
    }
    
    @Override
    public void remove(K key) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        dataMap.remove(key);
        lsnMap.remove(key);
        assert dataMap.size() == lsnMap.size();
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean replaceLsn(K key, V value, long newLsn) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        V current = dataMap.get(key);
        if (current == null) {
          return false;
        } else if (value.equals(current)) {
          lsnMap.put(key, newLsn);
          assert dataMap.size() == lsnMap.size();
          return true;
        } else {
          return false;
        }
      } finally {
        l.unlock();
      }
    }

  }
  
  static class Node<K, V> implements Comparable<Node<?, ?>> {

    private final K key;
    private final V value;
    private final long lsn;
    
    Node(K key, V value, long lsn) {
      this.key = key;
      this.value = value;
      this.lsn = lsn;
    }

    long getLsn() {
      return lsn;
    }
    
    K getKey() {
      return key;
    }
    
    V getValue() {
      return value;
    }

    Map.Entry<K, V> asEntry() {
      return new AbstractMap.SimpleEntry<K, V>(key, value);
    }
    
    @Override
    public int compareTo(Node<?, ?> t) {
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
}
