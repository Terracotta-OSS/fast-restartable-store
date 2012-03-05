/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.spi;

import com.terracottatech.fastrestartablestore.RecoveryFilter;
import java.util.Map.Entry;

import com.terracottatech.fastrestartablestore.messages.Action;

/**
 * @author cdennis
 */
public interface ObjectManager<I, K, V> {

  /**
   * @return lowest live lsn in system, -1 if none
   */
  long getLowestLsn();
  
//  long record(Action action, long lsn);
  
  RecoveryFilter createRecoveryFilter();
  
  long recordPut(I id, K key, long lsn);
  
  long recordRemove(I id, K key, long lsn);
  
  void recordDelete(I id, long lsn);
  
  void replayPut(I id, K key, V value, long lsn);
  
  void replayRemove(I id, K key, long lsn);
  
  void replayDelete(I id, long lsn);
  
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
  Action checkoutEarliest(long ceilingLsn);
  
  void checkin(Action action);

  /*
   * return map.size();
   */
  int size();
}
