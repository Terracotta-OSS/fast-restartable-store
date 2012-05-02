/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class NullAction implements Action {
  @Override
  public void record(long lsn) {
  }

  @Override
  public void replay(long lsn) {
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }
}
