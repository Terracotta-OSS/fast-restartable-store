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
  
  V get(K key);
  
  Long getLsn(K key);
  
  void put(K key, V value, long lsn);
  
  void replayPut(K key, V value, long lsn);
  
  void remove(K key);
  
  boolean replaceLsn(K key, V value, long newLsn);
}
