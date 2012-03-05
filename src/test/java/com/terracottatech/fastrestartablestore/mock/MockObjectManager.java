/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.terracottatech.fastrestartablestore.RecoveryFilter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
class MockObjectManager<I, K, V> implements ObjectManager<I, K, V> {

  private final Map<I, LinkedHashMap<K, Long>> map = new HashMap<I, LinkedHashMap<K, Long>>();

  private final Map<I, Map<K, V>> external;
  
  public MockObjectManager(Map<I, Map<K, V>> external) {
    this.external = external;
  }

  public long getLowestLsn() {
    Entry<K, Long> lowest = lowestEntry(Long.MAX_VALUE);
    if (lowest == null) {
      return -1;
    } else {
      return lowest.getValue();
    }
  }

  private IdEntry<I, K, Long> lowestEntry(long ceilingLsn) {
    IdEntry<I, K, Long> lowest = null;
    for (Entry<I, LinkedHashMap<K, Long>> m : map.entrySet()) {
      Iterator<Entry<K, Long>> it = m.getValue().entrySet().iterator();
      if (it.hasNext()) {
        Entry<K, Long> e = it.next();
        if (lowest == null || e.getValue() < lowest.getValue()) {
          lowest = new IdEntry<I, K, Long>(m.getKey(), e);
        }
      }
    }
    return lowest.getValue() < ceilingLsn ? lowest : null;
  }
  
  private static class IdEntry<I, K, V> extends AbstractMap.SimpleEntry<K, V> {

    private final I id;
    
    public IdEntry(I id, Entry<? extends K, ? extends V> arg0) {
      super(arg0);
      this.id = id;
    }
    
    public I getId() {
      return id;
    }
  }
  
  public Action checkoutEarliest(long ceilingLsn) {
  /*
   * while (true) {
   *   Entry<K, Long> entry = getFirstEntry();
   *   lock(entry.getKey());
   *   if (get(entry.getKey()) == entry.getValue()) {
   *     return external_map.getEntry(entry.getKey());
   *   } else {
   *     unlock(action.getKey());
   *   }
   * }  
   */
    
    IdEntry<I, K, Long> lowest = lowestEntry(ceilingLsn);
    if (lowest != null) {
      Map<K, V> m = external.get(lowest.getId());
      assert m != null;
      V value = m.get(lowest.getKey());
      assert value != null;
      return new MockPutAction<I, K, V>(lowest.getId(), lowest.getKey(), value);
    } else {
      return null;
    }
  }

  public void checkin(Action action) {
    //needs to unlock
  }

  public int size() {
    throw  new UnsupportedOperationException();
  }
  
  public long recordPut(I id, K key, long lsn) {
    LinkedHashMap<K, Long> m = map.get(id);
    if (m == null) {
      m = new LinkedHashMap<K, Long>();
      map.put(id, m);
    }
    Long previous = m.put(key, lsn);
    return previous == null ? -1 : previous;
  }

  @Override
  public long recordRemove(I id, K key, long lsn) {
    LinkedHashMap<K, Long> m = map.get(id);
    assert m != null;
    Long previous = m.remove(key);
    assert previous != null;
    return previous;
  }

  @Override
  public void recordDelete(I id, long lsn) {
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
    recordPut(id, key, lsn);
  }

  @Override
  public void replayRemove(I id, K key, long lsn) {
    
  }

  @Override
  public void replayDelete(I id, long lsn) {
    
  }

  @Override
  public RecoveryFilter createRecoveryFilter() {
    return new RecoveryFilter() {
      @Override
      public boolean replay(Action action, long lsn) {
        action.replay(MockObjectManager.this, lsn);
        return true;
      }
    };
  }
}
