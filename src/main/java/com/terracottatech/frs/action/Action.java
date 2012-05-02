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

  void record(long lsn);

  /**
   * Replay the given action.
   *
   * @param lsn lsn of the action
   */
  void replay(long lsn);

  ByteBuffer[] getPayload(ActionCodec codec);
}
