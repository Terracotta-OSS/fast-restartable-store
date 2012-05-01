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
import java.util.Collections;
import java.util.Set;

/**
 * @author tim
 */
class TransactionBeginAction implements Action {
  public static final TransactionLSNCallback NO_RECORD_CALLBACK = new TransactionLSNCallback() {
    @Override
    public void setLsn(TransactionHandle handle, long lsn) {
      throw new AssertionError();
    }
  };

  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              return new TransactionBeginAction(
                      new TransactionHandleImpl(ByteBufferUtils.getLong(buffers)), NO_RECORD_CALLBACK);
            }
          };

  private final TransactionHandle handle;
  private final TransactionLSNCallback callback;

  TransactionBeginAction(TransactionHandle handle, TransactionLSNCallback callback) {
    this.handle = handle;
    this.callback = callback;
  }

  TransactionHandle getHandle() {
    return handle;
  }

  @Override
  public void record(long lsn) {
    callback.setLsn(handle, lsn);
  }

  @Override
  public Set<Long> replay(long lsn) {
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

    return handle.equals(that.handle);
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
