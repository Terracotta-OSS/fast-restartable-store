/*
 * Copyright IBM Corp. 2024, 2025
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
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.transaction.TransactionAccount;
import com.terracottatech.frs.transaction.TransactionHandle;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface ActionManager {

  /**
   * Record the action and initiate a sync.
   *
   * @param action {@link Action} to record into the log stream
   * @return {@link Future} representing the disk write for the action.
   */
  Future<Void> syncHappened(Action action);

  /**
   * Record the given {@link Action} into the log stream.
   *
   * @param action {@link Action} to record
   * @return {@link Future} that represents when the {@link Action} is written to disk.
   */
  Future<Void> happened(Action action);

  /**
   * Record the given {@link Action} as part of an ongoing transaction and append it to the log stream.
   *
   * @param action  the {@link Action} to record; must not be {@code null}
   * @param handle  the {@link TransactionHandle} that identifies the enclosing transaction;
   *                must not be {@code null}
   * @param account the {@link TransactionAccount} for the enclosing transaction, used both to
   *                determine whether this is the first action in the transaction
   *                ({@link TransactionAccount#begin()}) and as the LSN callback; must not be
   *                {@code null}
   * @return a {@link Future} that completes when the log record has been written to disk
   */
  Future<Void> happenedTransactionally(Action action, TransactionHandle handle, TransactionAccount account);
  
  /**
   * Extract the {@link Action} from the given {@link LogRecord}
   *
   * @param record {@link LogRecord} to pull the {@link Action} out of.
   * @return {@link Action}
   */
  Action extract(LogRecord record);

  /**
   * Pause action manager.
   * <p>
   * On a return from this method, all {@link ActionManager#happened(Action)} and
   * {@link ActionManager#syncHappened(Action)} calls will block at entry, until the action manager
   * is resumed. This call comes out iff no more pending {@code happened()} and {@code syncHappened()} exists
   * in any threads and all incoming calls starts blocking, thereby guaranteeing that the gate is completely
   * closed.
   * 
   * @return the future that completes when the gating action is flushed to disk.
   */
  Future<Void> pause() throws InterruptedException;

  /**
   * Pause action manager.
   * <p>
   * On a return from this method, all {@link ActionManager#happened(Action)} and
   * {@link ActionManager#syncHappened(Action)} calls will block at entry, until the action manager
   * is resumed. This call comes out iff no more pending {@code happened()} and {@code syncHappened()} exists
   * in any threads and all incoming calls starts blocking, thereby guaranteeing that the gate is completely
   * closed and the action is appended to logrecord
   *
   * @return the future that completes when the action is flushed to disk.
   */
  Future<Void> syncHappenedAndPause(Action action) throws InterruptedException;
  
  /**
   * Resume action manager.
   * <p>
   * On a successful return, the {@link ActionManager} gate is open and all threads blocked in *happened() calls will
   * unblock itself and continue processing.
   */
  void resume();
}