/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.spi;

import com.terracottatech.fastrestartablestore.CompleteKey;

/**
 * @author cdennis
 */
public interface ObjectManager<I, K, V> {

  /**
   * @return lowest live lsn in system, -1 if none
   */
  long getLowestLsn();
  
  long getLsn(I id, K key);
  
  /*
   * XXX : do we want to have V here or not - should we make a decision on 
   * wrapping versus embedding of the final library.
   */
  long put(I id, K key, V value, long lsn);
  
  void delete(I id);
  
  long remove(I id, K key);

  void replayPut(I id, K key, V value, long lsn);

  //combination of id and key - it returns some composite object...
  CompleteKey<I, K> getCompactionKey();
  
  V replaceLsn(I id, K key, long newLsn);
}
