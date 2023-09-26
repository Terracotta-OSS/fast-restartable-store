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

import com.terracottatech.frs.log.LSNEventListener;

import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public interface Action extends LSNEventListener {

  /**
   * Called when the action has been assigned an LSN.
   *
   * @param lsn lsn assigned to this action
   */
  void record(long lsn);

  /**
   * Replay the given action.
   *
   * @param lsn lsn of the action
   */
  void replay(long lsn);

  /**
   * Determine the replay parallelism for this action. Multiple actions with the
   * same replay parallelism will be replayed sequentially.
   *
   * @return the replay parallelism number
   */
  default int replayConcurrency() {
    return 1;
  }

  /**
   * Get the serialized form of the action.
   *
   * @param codec {@link ActionCodec} to serialize the action with
   * @return Array of {@link ByteBuffer}s representing this action.
   */
  ByteBuffer[] getPayload(ActionCodec codec);
}
