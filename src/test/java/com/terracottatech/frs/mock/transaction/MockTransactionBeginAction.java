/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockTransactionBeginAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionBeginAction(long id) {
    this.id = id;
  }

  @Override
  public void record(long lsn) {
    // Nothing to do
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  public String toString() {
    return "Action: beginTransaction(" + id + ")";
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  public long getId() {
    return id;
  }
}
