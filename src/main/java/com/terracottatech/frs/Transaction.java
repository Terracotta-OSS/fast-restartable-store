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

  /**
   * Record a put
   *
   * @param id identifier to be put into
   * @param key key for the put
   * @param value value for the put
   * @return this
   * @throws TransactionException
   */
  Transaction<I, K, V> put(I id, K key, V value) throws TransactionException;

  /**
   * Record a delete on the id
   *
   * @param id identifier for the map to be deleted
   * @return this
   * @throws TransactionException
   */
  Transaction<I, K, V> delete(I id) throws TransactionException;

  /**
   * Record a remove
   *
   * @param id identifier to remove from
   * @param key key to be removed
   * @return this
   * @throws TransactionException
   */
  Transaction<I, K, V> remove(I id, K key) throws TransactionException;

  /**
   * Commit this transaction
   *
   * @throws TransactionException
   */
  void commit() throws TransactionException;
}
