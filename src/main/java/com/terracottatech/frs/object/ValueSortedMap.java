/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
