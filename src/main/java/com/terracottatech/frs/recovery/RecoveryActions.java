/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.ActionCodec;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public abstract class RecoveryActions {
  private RecoveryActions() {}

  public static void registerActions(int id, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    codec.registerAction(id, 0, RecoveryEvictionAction.class, RecoveryEvictionAction.FACTORY);
  }
}
