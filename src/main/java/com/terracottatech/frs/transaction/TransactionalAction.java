/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import static com.terracottatech.frs.util.ByteBufferUtils.concatenate;

/**
 * @author tim
 */
class TransactionalAction implements InvalidatingAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              return new TransactionalAction(
                      new TransactionHandleImpl(ByteBufferUtils.getLong(
                              buffers)), codec.decode(buffers));
            }
          };

  private final TransactionHandle handle;
  private final Action action;

  TransactionalAction(TransactionHandle handle, Action action) {
    this.handle = handle;
    this.action = action;
  }

  TransactionHandle getHandle() {
    return handle;
  }

  Action getAction() {
    return action;
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    if (action instanceof InvalidatingAction) {
      return ((InvalidatingAction) action).getInvalidatedLsns();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public void record(long lsn) {
    action.record(lsn);
  }

  @Override
  public Set<Long> replay(long lsn) {
    return action.replay(lsn);
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return action.lock(lockProvider);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return concatenate(handle.toByteBuffer(), codec.encode(action));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionalAction that = (TransactionalAction) o;

    return handle.equals(that.handle) && action.equals(that.action);
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
