/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class TransactionCommitAction implements Action {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              return new TransactionCommitAction(
                      new TransactionHandleImpl(ByteBufferUtils.getLong(buffers)));
            }
          };

  private final TransactionHandle handle;

  TransactionCommitAction(TransactionHandle handle) {
    this.handle = handle;
  }

  TransactionHandle getHandle() {
    return handle;
  }

  @Override
  public void record(long lsn) {

  }

  @Override
  public Set<Long> replay(long lsn) {
    return Collections.emptySet();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    return Collections.emptySet();
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[]{handle.toByteBuffer()};
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionCommitAction that = (TransactionCommitAction) o;

    return handle.equals(that.getHandle());
  }

  @Override
  public int hashCode() {
    return handle != null ? handle.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TransactionCommitAction{" +
            "handle=" + handle +
            '}';
  }
}
