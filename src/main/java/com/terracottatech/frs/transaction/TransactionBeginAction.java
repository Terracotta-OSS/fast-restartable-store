/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class TransactionBeginAction implements Action {
  private final TransactionHandle handle;

  TransactionBeginAction(TransactionHandle handle) {
    this.handle = handle;
  }

  @SuppressWarnings("unused")
  TransactionBeginAction(ObjectManager objectManager, ActionCodec codec, ByteBuffer[] buffers) {
    this(TransactionHandleImpl.withByteBuffers(buffers));
  }

  TransactionHandle getHandle() {
    return handle;
  }

  @Override
  public long getPreviousLsn() {
    return 0;
  }

  @Override
  public void record(long lsn) {

  }

  @Override
  public void replay(long lsn) {

  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return Collections.emptySet();
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[] { handle.toByteBuffer() };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionBeginAction that = (TransactionBeginAction) o;

    return !(handle != null ? !handle.equals(that.handle) : that.handle != null);
  }

  @Override
  public int hashCode() {
    return handle != null ? handle.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TransactionBeginAction{" +
            "handle=" + handle +
            '}';
  }
}
