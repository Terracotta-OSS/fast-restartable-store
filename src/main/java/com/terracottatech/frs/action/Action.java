/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
