/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 * 
 * @author cdennis
 */
public class MockTransactionalAction implements MockAction, Serializable,
        InvalidatingAction {
  private static final long serialVersionUID = 1L;
  private final long id;
  private final Action embedded;

  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (embedded instanceof MockAction) {
      ((MockAction) embedded).setObjectManager(objManager);
    }
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    if (embedded instanceof InvalidatingAction) {
      return ((InvalidatingAction) embedded).getInvalidatedLsns();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public void record(long lsn) {
    embedded.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

  long getId() {
    return id;
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  public Action getEmbeddedAction() {
    return embedded;
  }
}
