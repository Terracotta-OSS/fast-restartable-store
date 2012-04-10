/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author Chris Dennis
 */
public interface ObjectManagerSegment<I, K, V> {

  I identifier();

  K firstKey();
  
  Long firstLsn();
  
  Long getLsn(K key);
  
  void put(K key, V value, long lsn);
  
  void replayPut(K key, V value, long lsn);
  
  void remove(K key);
  
  V replaceLsn(K key, long newLsn);
}
