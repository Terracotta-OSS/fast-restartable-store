/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
