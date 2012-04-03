/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
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

  private final ConcurrentMap<I, List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>> maps = new ConcurrentHashMap<I, List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>>();
  private final int concurrency;
  
  public HeapObjectManager(int concurrency) {
    this.concurrency = concurrency;
  }
  
  
  @Override
  protected ConcurrentValueSortedMap<I, K, SequencedValue<V>> getStripeFor(I id, K key) {
    int spreadHash = spread(key.hashCode());
    List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> map = maps.get(id);
    if (map == null) {
      map = createStripes(id);
      List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> racer = maps.putIfAbsent(id, map);
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
  protected Collection<List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>> getStripes() {
    return maps.values();
  }
  
  private int spread(int hash) {
    return hash;
  }

  private List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> createStripes(I identifier) {
    List<ConcurrentValueSortedMap<I, K, SequencedValue<V>>> stripes = new ArrayList<ConcurrentValueSortedMap<I, K, SequencedValue<V>>>(concurrency);
    for (int i = 0; i < concurrency; i++) {
      stripes.add(new InHeapConcurrentValueSortedMap<I, K, SequencedValue<V>>(identifier));
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
  static class InHeapConcurrentValueSortedMap<I, K, V extends Comparable<V>> implements ConcurrentValueSortedMap<I, K, V> {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final I identifier;
    private final Map<K, Node<K, V>> map = new HashMap<K, Node<K, V>>();
    private final PriorityQueue<Node<K, V>> ordered = new PriorityQueue<Node<K, V>>();
            
    public InHeapConcurrentValueSortedMap(I identifier) {
      this.identifier = identifier;
    }
    
    @Override
    public I identifier() {
      return identifier;
    }

    @Override
    public Entry<K, V> firstEntry() {
      Lock l = lock.readLock();
      l.lock();
      try {
        Node peeked = ordered.peek();
        if (peeked == null) {
          assert map.size() == ordered.size();
          return null;
        } else {
          assert map.size() == ordered.size();
          return peeked.asEntry();
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
        Node<K, V> node = map.get(key);
        if (node == null) {
          assert map.size() == ordered.size();
          return null;
        } else {
          assert map.size() == ordered.size();
          return node.getValue();
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public void put(K key, V value) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        Node<K, V> node = new Node<K, V>(key, value);
        ordered.remove(map.put(key, node));
        ordered.add(node);
        assert map.size() == ordered.size();
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove(K key) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        ordered.remove(map.remove(key));
        assert map.size() == ordered.size();
      } finally {
        l.unlock();
      }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        Node<K, V> node = map.get(key);
        if (node == null) {
          assert map.size() == ordered.size();
          return false;
        } else if (oldValue.equals(node.getValue())) {
          Node<K, V> newNode = new Node<K, V>(key, newValue);
          ordered.remove(map.put(key, newNode));
          ordered.add(newNode);
          assert map.size() == ordered.size();
          return true;
        } else {
          assert map.size() == ordered.size();
          return false;
        }
      } finally {
        l.unlock();
      }
    }

  }
  
  static class Node<K, V extends Comparable<V>> implements Comparable<Node<?, V>> {

    private final K key;
    private final V value;
    
    Node(K key, V value) {
      this.key = key;
      this.value = value;
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
    public int compareTo(Node<?, V> t) {
      return value.compareTo(t.getValue());
    }
  }
}
