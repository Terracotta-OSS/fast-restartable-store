/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object.heap;

import com.terracottatech.frs.object.AbstractObjectManager;
import com.terracottatech.frs.object.AbstractObjectManagerStripe;
import com.terracottatech.frs.object.ObjectManagerSegment;
import com.terracottatech.frs.object.ObjectManagerStripe;
import com.terracottatech.frs.object.ValueSortedMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
public class HeapObjectManager<I, K, V> extends AbstractObjectManager<I, K, V> {

  private final ConcurrentMap<I, ObjectManagerStripe<I, K, V>> maps = new ConcurrentHashMap<I, ObjectManagerStripe<I, K, V>>();
  private final int concurrency;
  
  public HeapObjectManager(int concurrency) {
    this.concurrency = concurrency;
  }
  
  
  @Override
  protected ObjectManagerStripe<I, K, V> getStripeFor(I id) {
    ObjectManagerStripe<I, K, V> stripe = maps.get(id);
    if (stripe == null) {
      stripe = createStripes(id);
      ObjectManagerStripe<I, K, V> racer = maps.putIfAbsent(id, stripe);
      if (racer != null) {
        stripe = racer;
      }
    }
    return stripe;
  }

  @Override
  protected void deleteStripeFor(I id) {
    maps.remove(id);
  }

  @Override
  protected Collection<ObjectManagerStripe<I, K, V>> getStripes() {
    return maps.values();
  }
  
  private ObjectManagerStripe<I, K, V> createStripes(I identifier) {
    return new InHeapObjectManagerStripe<I, K, V>(identifier, concurrency);
  }
  
  static class InHeapObjectManagerStripe<I, K, V> extends AbstractObjectManagerStripe<I, K, V> {
    
    private final ObjectManagerSegment<I, K, V>[] segments;

    @SuppressWarnings("unchecked")
    public InHeapObjectManagerStripe(I identifier, int stripes) {
      this.segments = new ObjectManagerSegment[stripes];
      for (int i = 0; i < segments.length; i++) {
        segments[i] = new InHeapObjectManagerSegment<I, K, V>(identifier);
      }
    }
    
    @Override
    public Collection<ObjectManagerSegment<I, K, V>> getSegments() {
      return Arrays.asList(segments);
    }

    protected ObjectManagerSegment<I, K, V> getSegmentFor(K key) {
      int hash = key.hashCode();
      return segments[Math.abs(hash % segments.length)];
    }
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
    private final ValueSortedMap<K, Long> lsnMap = new HeapValueSortedMap<K, Long>();
    
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
    public V replaceLsn(K key, long newLsn) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        V current = dataMap.get(key);
        if (current == null) {
          return null;
        } else {
          lsnMap.put(key, newLsn);
          assert dataMap.size() == lsnMap.size();
          return current;
        }
      } finally {
        l.unlock();
      }
    }

  }
}