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
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LogRecord;

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
   */
  void pause();

  /**
   * Resume action manager.
   * <p>
   * On a successful return, the {@link ActionManager} gate is open and all threads blocked in *happened() calls will
   * unblock itself and continue processing.
   */
  void resume();

  /**
   * Return a dummy barrier action as a log record that can be used as a freeze marker.
   *
   * @return a dummy Log record that can be used as a freeze marker
   */
  LogRecord barrierAction();
}