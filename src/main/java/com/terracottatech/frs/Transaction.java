/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs;

/**
 *
 * @author cdennis
 */
public interface Transaction<I, K, V> {

  Transaction<I, K, V> put(I id, K key, V value);
  
  Transaction<I, K, V> delete(I id);

  Transaction<I, K, V> remove(I id, K key);

  void commit();
  
}
