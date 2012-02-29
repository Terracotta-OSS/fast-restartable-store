/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
class MockObjectManager<K, V> implements ObjectManager {

  private final Lock lock = new ReentrantLock();
  
  private final LinkedHashMap<K, Long> map = new LinkedHashMap<K, Long>();

  private Map<K, V> external;
  
  public MockObjectManager(Map<K, V> external) {
    this.external = external;
  }

  public long getLowestLsn() {
    Iterator<Long> it = map.values().iterator();
    if (it.hasNext()) {
      return it.next().longValue();
    } else {
      return -1L;
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
    
    Iterator<Entry<K, Long>> it = map.entrySet().iterator();
    if (it.hasNext()) {
      Entry<K, Long> e = it.next();
      if (e.getValue() >= ceilingLsn) {
         return null;
      }
      V value = external.get(e.getKey());
      if (value == null) {
        return null;
      } else {
        return new MockPutAction<K, V>(e.getKey(), value);
      }
    } else {
      return null;
    }
  }

  public void checkin(Action action) {
    //needs to unlock
  }

  public int size() {
    return map.size();
  }

@Override
public long record(Action action, long lsn) {
   if (action instanceof MockPutAction) {
      Long old = map.put(((MockPutAction<K,V>) action).getKey(), lsn);
      if (old == null) {
         return -1;
      } else {
         return old;
      }
   } else if (action instanceof MockRemoveAction) {
      Long old = map.remove(((MockRemoveAction<K>) action).getKey());
      if (old == null) {
         return -1;
      } else {
         return old;
      }
   } else {
      throw new IllegalArgumentException("Unknown action " + action);
   }
}

@Override
public boolean replay(Action action, long lsn) {
   if (action instanceof MockPutAction) {
      record(action, lsn);
      external.put(((MockPutAction<K, V>) action).getKey(), ((MockPutAction<K, V>) action).getValue());
      return true;
   } else if (action instanceof MockRemoveAction) {
      return true;
   } else {
      throw new IllegalArgumentException("Unknown action " + action);
   }
}
}
