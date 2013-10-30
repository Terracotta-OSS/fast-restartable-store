/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author cdennis
 */
public interface ValueSortedMap<K, V extends Comparable<V>> {

  K firstKey();
  
  V firstValue();
  
  public void put(K key, V value);

  public void remove(K key);

  public V get(K key);
  
  public int size();
}
