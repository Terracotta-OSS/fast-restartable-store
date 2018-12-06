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

  /**
   * Start the process of pausing incoming actions. Also, start the process of pausing compaction and return
   * a {@link Future} that can be used to check when the pause completes.
   *
   * The pause action is complete when all ongoing actions are queued (including compaction) and new actions
   * are frozen, thereby closing the gate for any future actions, until either a resume is called or until
   * an internal timeout happens. Just before completing the pause action, as snapshot request will be queued
   * and a future to this snapshot request will be returned.
   * <p>
   * A snapshot request takes a snapshot of this {@link RestartStore} for backup purposes. Once the snapshot completes
   * ({@link Future#get()} all transactions that have already been committed prior to the snapshot call are guaranteed
   * to be in the snapshot. Changes made while the snapshot is taken may or may not be in the snapshot. The snapshot
   * must be released after it's used in order to release any held resources and to unpause compaction.
   *
   * @return {@link Future} that completes when the pause is complete (including compaction pause and snapshot request
   *         queueing).
   */
  Future<Future<Snapshot>> pause();

  /**
   * Resume queueing of incoming actions for IO.
   *
   * @throws NotPausedException, if the store is NOT in a paused state.
   */
  void resume() throws NotPausedException;

  /**
   * Start the process of freezing incoming actions. Also, start the process of pausing compaction and return
   * a {@link Future} that can be used to check when the pause completes.
   *
   * The freeze action is complete when all ongoing actions are queued (including compaction) and new actions
   * are frozen, thereby closing the gate for any future actions forever.
   *
   * Once the gate is closed for new actions, a {@Link NullAction} will be queued and a future to this null action
   * request will be returned. Waiting on that future ensures not only that data up to that marker has reached
   * durable storage but also that all other subsequent actions are frozen forever.
   * <p>
   * Once a freeze is complete, either it can be resumed or the JVM system can be stopped/exited in this freeze state.
   * <p>
   * The returned future of future allows clients to enable a two phase scheme where a freeze can be invoked on
   * multiple stores simultaneously and then waited for so that multiple restart stores will completely flush and
   * close the gates in a consistent fashion across multiple nodes and stores.
   *
   * @return {@link Future} that completes when the freeze is complete.
   */
  Future<Future<Void>> freeze();
}