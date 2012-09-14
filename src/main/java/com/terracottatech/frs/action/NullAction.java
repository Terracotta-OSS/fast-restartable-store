/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class NullAction implements Action {
  public static final Action INSTANCE = new NullAction();
  
  private long lsn;

  public static <I, K, V> ActionFactory<I, K, V> factory() {
    return new ActionFactory<I, K, V>() {
      @Override
      public Action create(ObjectManager<I, K, V> objectManager, ActionCodec codec, ByteBuffer[] buffers) {
        return INSTANCE;
      }
    };
  }

  @Override
  public void record(long lsn) {
      this.lsn = lsn;
  }
  
  public long getLsn() {
      return lsn;
  }

  @Override
  public void replay(long lsn) {
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }
}
