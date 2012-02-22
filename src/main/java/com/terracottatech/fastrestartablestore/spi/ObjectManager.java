/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.spi;

import com.terracottatech.fastrestartablestore.messages.KeyedAction;
import java.util.Map.Entry;
import java.util.logging.LogRecord;

/**
 * @author cdennis
 */
public interface ObjectManager<K, V> {

  /*
   * Long oldLsn = objectManager.put(action.getKey(), newLsn);
   */
  <Long lowest, Long previous> put(K key, Long lsn);
  
  <Long lowest, Long previous> remove(K key);
  
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
