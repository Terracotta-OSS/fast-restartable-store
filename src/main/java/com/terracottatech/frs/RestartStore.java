/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.recovery.RecoveryException;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface RestartStore<I, K, V> {

  /**
   * Start the {@link RestartStore} for operation returning a {@link Future} representing the state of the
   * recovery process.
   *
   * @return {@link Future} that completes when recovery is completed.
   * @throws InterruptedException
   */
  Future<Void> startup() throws InterruptedException, RecoveryException;

  /**
   * Cleanly shut down the {@link RestartStore}. All in flight operations will be allowed
   * to finish, and their results will be flushed to stable storage.
   *
   * @throws InterruptedException
   */
  void shutdown() throws InterruptedException;

  /**
   * Open a transaction for mutating the {@link RestartStore}
   *
   * @return a transaction context
   */
  Transaction<I, K, V> beginTransaction();

  /**
   * Open an auto-commit transaction.
   *
   * @return an auto-commit transaction context.
   */
  Transaction<I, K, V> beginAutoCommitTransaction();
}
