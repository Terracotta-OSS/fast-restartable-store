/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Set;

/**
 *
 * @author Chris Dennis
 */
public interface ObjectManagerSegment<I, K, V> {

  I identifier();

  K firstKey();
  
  Long getLowestLsn();
  
  Long getLsn(int hash, K key);
  
  void put(int hash, K key, V value, long lsn);
  
  Set<Long> replayPut(int hash, K key, V value, long lsn);
  
  void remove(int hash, K key);
  
  V replaceLsn(int hash, K key, long newLsn);

  long size();
}
