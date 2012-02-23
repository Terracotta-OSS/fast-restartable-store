/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author cdennis
 */
class MockObjectManager implements ObjectManager<String, String> {

  private final Lock lock = new ReentrantLock();
  
  private final LinkedHashMap<String, Long> map = new LinkedHashMap();

  public MockObjectManager() {
  }

  public long getLowestLsn() {
    Iterator<Long> it = map.values().iterator();
    if (it.hasNext()) {
      return it.next().longValue();
    } else {
      return -1L;
    }
  }

  public long updateLsn(String key, long lsn) {
    Long old = map.put(key, lsn);
    if (old == null) {
      return -1L;
    } else {
      return old.longValue();
    }
  }

  public void replayPut(String key, String value, long lsn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public void replayRemove(String key, long lsn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public Entry checkoutEarliest() {
    Iterator<Entry<String, Long>> it = map.entrySet().iterator();
    if (it.hasNext()) {
      return it.next();
    } else {
      return null;
    }
  }

  public void checkin(Entry entry) {
    //needs to unlock
  }

  public int size() {
    return map.size();
  }
  
}
