/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.log.LSNEventListener;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public interface Action extends LSNEventListener {

  void record(long lsn);

  /**
   * Replay the given action. Also returns a set of LSNs that this action may
   * have invalidated.
   *
   * @param lsn lsn of the action
   * @return invalidated lsns
   */
  Set<Long> replay(long lsn);

  ByteBuffer[] getPayload(ActionCodec codec);
}
