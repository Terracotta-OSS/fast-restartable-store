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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

import static com.terracottatech.frs.util.ByteBufferUtils.concatenate;
import static com.terracottatech.frs.util.ByteBufferUtils.get;

/**
 * @author tim
 */
class TransactionalAction implements InvalidatingAction, TransactionAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              return new TransactionalAction(
                      TransactionHandleImpl.withByteBuffers(buffers), get(buffers), codec.decode(buffers));
            }
          };
  
  private static final byte COMMIT_BIT = 0x01;
  private static final byte BEGIN_BIT = 0x02;

  private final TransactionHandle handle;
  private final Action action;
  private final byte mode;
  private final TransactionLSNCallback callback;

  TransactionalAction(TransactionHandle handle, byte mode, Action action) {
    this.handle = handle;
    this.action = action;
    this.mode = mode;
    this.callback = null;
  }
  
  TransactionalAction(TransactionHandle handle, boolean begin, boolean commit, Action action, TransactionLSNCallback callback) {
    this.handle = handle;
    this.action = action;
    byte tempMode = 0;
    if (commit) {
      tempMode |= COMMIT_BIT;
    }
    if (begin) {
      tempMode |= BEGIN_BIT;
    }
    mode = tempMode;
    this.callback = callback;
  }

  @Override
  public boolean isCommit() {
    return (mode & COMMIT_BIT) != 0;
  }

  @Override
  public boolean isBegin() {
    return (mode & BEGIN_BIT) != 0;
  }

  public TransactionHandle getHandle() {
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
    assert callback != null;
    action.record(lsn);
    callback.setLsn(lsn);
  }

  @Override
  public void replay(long lsn) {
    action.replay(lsn);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer handleBuffer = handle.toByteBuffer();
    ByteBuffer header = ByteBuffer.allocate(handleBuffer.capacity() + 1);
    header.put(handleBuffer).put(mode).flip();
    return concatenate(header, codec.encode(action));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionalAction that = (TransactionalAction) o;

    return handle.equals(that.handle) && action.equals(that.action) && mode == that.mode;
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
