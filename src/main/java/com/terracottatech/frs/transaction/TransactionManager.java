/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
   * @throws InterruptedException if the committing thread is interrupted
   * @throws TransactionException thrown if the flush fails for some reason
   */
  void commit(TransactionHandle handle) throws InterruptedException, TransactionException;

  /**
   * Record the {@link Action} under the given transaction
   *
   * @param handle handle to the transaction the action will happen under
   * @param action the {@link Action} to record
   */
  void happened(TransactionHandle handle, Action action);

  /**
   * Record an {@link Action} with no associated transaction.
   *
   *
   * @param action {@link com.terracottatech.frs.action.Action} to recorded.
   * @throws InterruptedException if the committing thread is interrupted
   * @throws TransactionException thrown if the flush fails for some reason
   */
  void happened(Action action) throws InterruptedException, TransactionException;
}
