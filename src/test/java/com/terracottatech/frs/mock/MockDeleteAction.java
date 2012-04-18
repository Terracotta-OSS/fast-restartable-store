/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.object.ObjectManager;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

public class MockDeleteAction<I> implements MockAction, Serializable {

  private final I id;
  private transient ObjectManager<I, ?, ?> objManager;

  public MockDeleteAction(ObjectManager<I, ?, ?> objManager, I id) {
    this.id = id;
    this.objManager = objManager;
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, ?, ?>) objManager;
  }

  @Override
  public void record(long lsn) {
    objManager.delete(id);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    Lock idLock = locks.getLockForId(id).writeLock();
    idLock.lock();
    return Collections.singleton(idLock);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  public I getId() {
    return id;
  }
}
