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
public interface ObjectManager {

  /**
   * @return lowest live lsn in system, -1 if none
   */
  long getLowestLsn();
  
  long record(Action action, long lsn);
  
  RecoveryFilter createRecoveryFilter();
  
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
