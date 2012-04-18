/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class RemoveAction implements Action {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              int idLength = ByteBufferUtils.getInt(buffers);
              int keyLength = ByteBufferUtils.getInt(buffers);
              return new RemoveAction(objectManager, ByteBufferUtils.getBytes(idLength, buffers),
                                      ByteBufferUtils.getBytes(keyLength, buffers));
            }
          };

  private static final int HEADER_SIZE = ByteBufferUtils.INT_SIZE * 2;

  private final ObjectManager<ByteBuffer, ByteBuffer, ?> objectManager;
  private final ByteBuffer id;
  private final ByteBuffer key;

  RemoveAction(ObjectManager<ByteBuffer, ByteBuffer, ?> objectManager, ByteBuffer id, ByteBuffer key) {
    this.objectManager = objectManager;
    this.id = id;
    this.key = key;
  }

  @Override
  public long getPreviousLsn() {
    return objectManager.getLsn(id, key);
  }

  @Override
  public void record(long lsn) {
    objectManager.remove(id, key);
  }

  @Override
  public void replay(long lsn) {
    // Nothing to remove on replay
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForKey(id, key).writeLock();
    lock.lock();
    return Collections.singleton(lock);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    // TODO: Can we just return no data for remove? It's not technically necessary to
    // write anything to the log since nothing needs to be replayed.
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    header.putInt(id.remaining());
    header.putInt(key.remaining()).flip();
    return new ByteBuffer[] { header, id.slice(), key.slice() };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RemoveAction that = (RemoveAction) o;

    return id.equals(that.id) && key.equals(that.key);
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    return result;
  }
}
