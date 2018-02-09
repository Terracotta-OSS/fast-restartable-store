package com.terracottatech.frs.object;

import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.object.heap.HeapValueSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class RestartableMap<K, V, RI, RK, RV> implements ConcurrentMap<K, V>, RestartableObject<RI, RK, RV> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestartableMap.class);

  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ObjectManagerStripe<RI, RK, RV> objectManagerStripe = new MapObjectManagerStripe();
  private final RestartStore<RI, RK, RV> restartability;
  private final RI identifier;
  private final long identifierByteSize;
  
  private final Map<K, V> dataMap = new ConcurrentHashMap<K, V>();
  private final HeapValueSortedMap<K, Long> lsnMap = new HeapValueSortedMap<K, Long>();

  private final boolean synchronousWrites;
  
  private long byteSize = 0;
  
  private Set<K> keySet;
  private Set<Entry<K, V>> entrySet;

  public RestartableMap(RI identifier, RestartStore<RI, RK, RV> restartability, boolean synchronousWrites, int identifierByteSize) {
    this.identifier = identifier;
    this.restartability = restartability;
    this.synchronousWrites = synchronousWrites;
    this.identifierByteSize = identifierByteSize;
  }

  public RestartableMap(RI identifier, RestartStore<RI, RK, RV> restartability, boolean synchronousWrites) {
    this.identifier = identifier;
    this.restartability = restartability;
    this.synchronousWrites = synchronousWrites;

    if (identifier instanceof ByteBuffer) {
      identifierByteSize = ((ByteBuffer) identifier).remaining();
    } else {
      LOGGER.warn("Strange identifier: expected: " + ByteBuffer.class + " found: " + identifier.getClass());
      identifierByteSize = 0;
    }
  }
  
  public RestartableMap(RI identifier, RestartStore<RI, RK, RV> restartability) {
    this(identifier, restartability, true);
  }

  public RI getId() {
    return identifier;
  }
  
  public ObjectManagerStripe<RI, RK, RV> getObjectManagerStripe() {
    return objectManagerStripe;
  }

  @Override
  public int size() {
    Lock l = lock.readLock();
    l.lock();
    try {
      return dataMap.size();
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    Lock l = lock.readLock();
    l.lock();
    try {
      return dataMap.isEmpty();
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean containsKey(Object key) {
    Lock l = lock.readLock();
    l.lock();
    try {
      return dataMap.containsKey(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean containsValue(Object value) {
    Lock l = lock.readLock();
    l.lock();
    try {
      return dataMap.containsValue(value);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V get(Object key) {
    Lock l = lock.readLock();
    l.lock();
    try {
      return dataMap.get(key);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V put(K key, V value) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      V old = dataMap.put(key, value);
      RK encodedKey = encodeKey(key);
      RV encodedValue = encodeValue(value);
      if(old == null) {
        byteSize += identifierByteSize + keyByteSize(key, encodedKey) + valueByteSize(value, encodedValue);
      } else {
        byteSize += valueByteSize(value, encodedValue) - valueByteSize(old, encodeValue(old));
      }
      
      restartability.beginTransaction(synchronousWrites).put(identifier, encodedKey, encodedValue).commit();
      return old;
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    } finally {
      l.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public V remove(Object key) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      V removed = dataMap.remove(key);
      if (removed != null) {
        RK encodedKey = encodeKey((K) key);
        byteSize -= identifierByteSize + keyByteSize((K) key, encodedKey) + valueByteSize(removed, encodeValue(removed));
        restartability.beginTransaction(synchronousWrites).remove(identifier, encodedKey).commit();
      }
      return removed;
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Entry<? extends K, ? extends V> e : m.entrySet()) {
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public void clear() {
    Lock l = lock.writeLock();
    l.lock();
    try {
      dataMap.clear();
      restartability.beginTransaction(synchronousWrites).delete(identifier).commit();
      byteSize = 0;
    } catch (TransactionException e) {
      throw new RuntimeException(e);
    } finally {
      l.unlock();
    }
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      V old = get(key);
      if (old == null) {
        put(key, value);
      }
      return old;
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean remove(Object key, Object value) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      if (value.equals(get(key))) {
        remove(key);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      if (oldValue.equals(get(key))) {
        put(key, newValue);
        return true;
      } else {
        return false;
      }
    } finally {
      l.unlock();
    }
  }

  @Override
  public V replace(K key, V value) {
    Lock l = lock.writeLock();
    l.lock();
    try {
      V old = get(key);
      if (old != null) {
        put(key, value);
      }
      return old;
    } finally {
      l.unlock();
    }
  }

  @Override
  public Set<K> keySet() {
    Set<K> ks = keySet;
    return ks != null ? ks : (keySet = new LockedSet<K>(dataMap.keySet()));
  }

  @Override
  public Collection<V> values() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> es = entrySet;
    return es != null ? es : (entrySet = new LockedSet<Entry<K, V>>(dataMap.entrySet()));
  }

  protected void replayPut(K key, V value) {
    dataMap.put(key, value);
  }
  
  protected abstract RK encodeKey(K key);
  
  protected abstract RV encodeValue(V value);
  
  protected abstract K decodeKey(RK rKey);
  
  protected abstract V decodeValue(RV rValue);
  
  protected abstract long keyByteSize(K key, RK encodedKey);
  
  protected abstract long valueByteSize(V value, RV encodedValue);

  private final class LockedSet<T> extends AbstractSet<T> {

    private final Set<T> delegate;

    LockedSet(Set<T> delegate) {
      this.delegate = delegate;
    }
    
    public Iterator<T> iterator() {
      Lock l = lock.readLock();
      l.lock();
      try {
        return new Iterator<T>() {
          
          private final Iterator<T> delegateIterator = delegate.iterator();
          
          @Override
          public boolean hasNext() {
            Lock lk = lock.readLock();
            lk.lock();
            try {
              return delegateIterator.hasNext();
            } finally {
              lk.unlock();
            }
          }
  
          @Override
          public T next() {
            Lock lk = lock.readLock();
            lk.lock();
            try {
              return delegateIterator.next();
            } finally {
              lk.unlock();
            }
          }
  
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      } finally {
        l.unlock();
      }
    }
    
    public boolean contains(Object o) {
      Lock l = lock.readLock();
      l.lock();
      try {
        return delegate.contains(o);
      } finally {
        l.unlock();
      }
    }
    public boolean remove(Object o) {
      throw new UnsupportedOperationException();
    }
    
    public int size() {
      Lock l = lock.readLock();
      l.lock();
      try {
        return delegate.size();
      } finally {
        l.unlock();
      }
    }
    
    public void clear() {
      throw new UnsupportedOperationException();
    }
  }
  
  private final class MapObjectManagerStripe implements ObjectManagerStripe<RI, RK, RV>, ObjectManagerSegment<RI, RK, RV> {

    @Override
    public Long getLowestLsn() {
      Lock l = lock.readLock();
      l.lock();
      try {
        return lsnMap.firstValue();
      } finally {
        l.unlock();
      }
    }

    @Override
    public Long getLsn(RK key) {
      Lock l = lock.readLock();
      l.lock();
      try {
        return lsnMap.get(decodeKey(key));
      } finally {
        l.unlock();
      }
    }

    @Override
    public void put(RK rKey, RV rValue, long lsn) {
      K key = decodeKey(rKey);
      Lock l = lock.writeLock();
      l.lock();
      try {
        if (dataMap.containsKey(key)) {
          lsnMap.put(key, lsn);
        } else {
          throw new AssertionError();
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public void remove(RK rKey) {
      K key = decodeKey(rKey);
      Lock l = lock.writeLock();
      l.lock();
      try {
        if (dataMap.containsKey(key)) {
          throw new AssertionError();
        } else {
          lsnMap.remove(key);
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public void delete() {
      Lock l = lock.writeLock();
      l.lock();
      try {
        if (dataMap.isEmpty()) {
          lsnMap.clear();
        } else {
          throw new AssertionError();
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public void replayPut(RK rKey, RV rValue, long lsn) {
      K key = decodeKey(rKey);
      V value = decodeValue(rValue);

      Lock l = lock.writeLock();
      l.lock();
      try {
        if (dataMap.containsKey(key)) {
          throw new AssertionError();
        } else {
          RestartableMap.this.replayPut(key, value);
          byteSize += identifierByteSize + keyByteSize(key, rKey) + valueByteSize(value, rValue);
          lsnMap.put(key, lsn);
        }
      } finally {
        l.unlock();
      }
    }

    @Override
    public Collection<ObjectManagerSegment<RI, RK, RV>> getSegments() {
      return Collections.<ObjectManagerSegment<RI, RK, RV>>singleton(this);
    }

    @Override
    public void updateLsn(ObjectManagerEntry<RI, RK, RV> entry, long newLsn) {
      K key = decodeKey(entry.getKey());
      if (entry.getLsn() == lsnMap.get(key)) {
        lsnMap.put(key, newLsn);
      } else {
        throw new AssertionError();
      }
    }

    @Override
    public ObjectManagerEntry<RI, RK, RV> acquireCompactionEntry(long ceilingLsn) {
      Lock l = lock.writeLock();
      l.lock();
      try {
        K key = lsnMap.firstKey();
        if (key == null) {
          l.unlock();
          return null;
        }
        long lsn = lsnMap.firstValue();
        if (lsn >= ceilingLsn) {
          l.unlock();
          return null;
        }
        RK rKey = encodeKey(key);
        RV rValue = encodeValue(dataMap.get(key));
        return new SimpleObjectManagerEntry<RI, RK, RV>(identifier, rKey, rValue, lsn);
      } catch (RuntimeException e) {
        l.unlock();
        throw e;
      } catch (Error e) {
        l.unlock();
        throw e;
      }
    }

    @Override
    public void releaseCompactionEntry(ObjectManagerEntry<RI, RK, RV> entry) {
      if (entry == null) {
        throw new NullPointerException("Tried to release a null entry.");
      } else {
        lock.writeLock().unlock();
      }
    }

    @Override
    public long size() {
      Lock l = lock.readLock();
      l.lock();
      try {
        return lsnMap.size();
      } finally {
        l.unlock();
      }
    }

    @Override
    public long sizeInBytes() {
      Lock l = lock.writeLock();
      l.lock();
      try {
        return byteSize;
      } finally {
        l.unlock();
      }
    }

    @Override
    public void updateLsn(int hash, ObjectManagerEntry<RI, RK, RV> entry, long newLsn) {
      updateLsn(entry, newLsn);
    }

    @Override
    public Long getLsn(int hash, RK key) {
      return getLsn(key);
    }

    @Override
    public void put(int hash, RK key, RV value, long lsn) {
      put(key, value, lsn);
    }

    @Override
    public void replayPut(int hash, RK key, RV value, long lsn) {
      replayPut(key, value, lsn);
    }

    @Override
    public void remove(int hash, RK key) {
      remove(key);
    }
  }
}
