/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

/**
 *
 * @author cdennis
 */
public interface Transaction<I, K, V> {

  void put(I id, K key, V value);
  
  void delete(I id);

  void remove(I id, K key);

  void commit();
  
}
