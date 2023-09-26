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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;

/**
 *
 * @author cdennis
 */
public interface TransactionManager {

  /**
   * Begin a transaction and return a {@link TransactionHandle} for the new transaction.
   *
   * @return handle to the newly created transaction
   */
  TransactionHandle begin();

  /**
   * Commit the transaction
   *
   * @param handle handle to the transaction to commit.
   * @param synchronous whether or not to commit this transaction synchronously
   * @throws TransactionException thrown if the flush fails for some reason
   */
  void commit(TransactionHandle handle, boolean synchronous) throws TransactionException;

  /**
   * Record the {@link Action} under the given transaction
   *
   * @param handle handle to the transaction the action will happen under
   * @param action the {@link Action} to record
   */
  void happened(TransactionHandle handle, Action action);

  /**
   * Get the beginning LSN of the lowest open transaction in the system.
   *
   * @return lowest transaciton begin LSN of a live transaction
   */
  long getLowestOpenTransactionLsn();
}
