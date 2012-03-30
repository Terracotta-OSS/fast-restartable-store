/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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

  void commit() throws InterruptedException, TransactionException;
  
}
