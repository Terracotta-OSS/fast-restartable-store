/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionDecodeException;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class TransactionalAction implements Action {
  private final TransactionHandle handle;
  private final Action action;

  TransactionalAction(TransactionHandle handle, Action action) {
    this.handle = handle;
    this.action = action;
  }

  TransactionalAction(ObjectManager objectManager, ActionCodec codec, ByteBuffer[] buffers) throws
          ActionDecodeException {
    this(TransactionHandleImpl.withByteBuffers(buffers), codec.decode(buffers));
  }

  TransactionHandle getHandle() {
    return handle;
  }

  Action getAction() {
    return action;
  }

  @Override
  public long getPreviousLsn() {
    return action.getPreviousLsn();
  }

  @Override
  public void record(long lsn) {
    action.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    action.replay(lsn);
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return action.lock(lockProvider);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer[] embeddedPayload = codec.encode(action);
    ByteBuffer[] payload = new ByteBuffer[embeddedPayload.length + 1];
    payload[0] = handle.toByteBuffer();
    System.arraycopy(embeddedPayload, 0, payload, 1, embeddedPayload.length);
    return payload;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionalAction that = (TransactionalAction) o;

    if (action != null ? !action.equals(that.action) : that.action != null) return false;
    return !(handle != null ? !handle.equals(that.handle) : that.handle != null);
  }

  @Override
  public int hashCode() {
    int result = handle != null ? handle.hashCode() : 0;
    result = 31 * result + (action != null ? action.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TransactionalAction{" +
            "handle=" + handle +
            ", action=" + action +
            '}';
  }
}
