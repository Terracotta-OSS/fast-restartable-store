/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

/**
 *
 * @author cdennis
 */
public interface TransactionContext<K, V> {

  void put(K key, V value);

  void remove(K key);

  void commit();
  
}
