/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

import java.util.Map.Entry;

/**
 *
 * @author Chris Dennis
 */
interface ConcurrentValueSortedMap<I, K, V> {

  I identifier();

  Entry<K, V> firstEntry();
  
  V get(K key);
  
  void put(K key, V value);
  
  void remove(K key);
  
  boolean replace(K key, V oldValue, V newValue);
}
