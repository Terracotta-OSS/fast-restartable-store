/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;

import java.util.concurrent.Future;

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
   * @throws InterruptedException if the committing thread is interrupted
   * @throws TransactionException thrown if the flush fails for some reason
   */
  void commit(TransactionHandle handle, boolean synchronous) throws InterruptedException, TransactionException;

  /**
   * Record the {@link Action} under the given transaction
   *
   * @param handle handle to the transaction the action will happen under
   * @param action the {@link Action} to record
   */
  void happened(TransactionHandle handle, Action action);

  /**
   * Record an {@link Action} synchronously with no associated transaction.
   *
   *
   * @param action {@link com.terracottatech.frs.action.Action} to recorded.
   * @throws InterruptedException if the committing thread is interrupted
   * @throws TransactionException thrown if the flush fails for some reason
   */
  void happened(Action action) throws InterruptedException, TransactionException;

  /**
   * Performs an {@link Action} asynchronously with no associated transaction.
   *
   * @param action action that happened
   * @return {@link Future} representing when the action has been flushed to disk
   */
  Future<Void> asyncHappened(Action action);

  /**
   * Get the beginning LSN of the lowest open transaction in the system.
   *
   * @return lowest transaciton begin LSN of a live transaction
   */
  long getLowestOpenTransactionLsn();
}
