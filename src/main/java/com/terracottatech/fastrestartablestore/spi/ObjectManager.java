/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.spi;

import java.util.Map.Entry;

/**
 * @author cdennis
 */
public interface ObjectManager<K, V> {

  /**
   * @return lowest live lsn in system, -1 if none
   */
  long getLowestLsn();

  /**
   * return map.put(key, lsn);
   * 
   * @return previous lsn for this key, -1 if unknown
   */
  long updateLsn(K key, long lsn);
  
  void replayPut(K key, V value, long lsn);
  void replayRemove(K key, long lsn);
  
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
  Entry<K, V> checkoutEarliest();
  
  void checkin(Entry<K, V> entry);

  /*
   * return map.size();
   */
  int size();
}
