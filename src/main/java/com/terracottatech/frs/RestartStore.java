/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.io.IOStatistics;
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
   * @param synchronous whether or not the transaction should be committed synchronously
   * @return a transaction context
   */
  Transaction<I, K, V> beginTransaction(boolean synchronous);

  /**
   * Open an auto-commit transaction.
   *
   * @param synchronous whether the actions within the autocommit transaction should be
   *                    committed synchronously
   * @return an auto-commit transaction context.
   */
  Transaction<I, K, V> beginAutoCommitTransaction(boolean synchronous);
  
  /**
   * randomly access a record from the log at a user provided marker
   *
   * @param marker the marker which was provided at put time
   * @return a tuple representing the action placed in the log, null if the action represented at
   *      the requested marker is not gettable or the marker does not exist in the log
   * 
   */
  Tuple<I, K, V> get(long marker);

  /**
   * Take a snapshot of this {@link RestartStore} for backup purposes. All transactions that have already been committed
   * prior to the snapshot call are guaranteed to be in the snapshot. Changes made while the snapshot is taken may or may
   * not be in the snapshot. The snapshot must be released after it's used in order to release any held resources.
   */
  Snapshot snapshot() throws RestartStoreException;
  
  /**
   * get statistics from the underlying implementation.  
   * 
   * 
   * @return statistics from current log stream implementation 
   */
  Statistics getStatistics();
}
