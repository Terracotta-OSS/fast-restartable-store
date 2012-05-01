/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * @author tim
 */
class DeleteAction implements Action {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY = new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
    @Override
    public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                         ActionCodec codec, ByteBuffer[] buffers) {
      int idLength = ByteBufferUtils.getInt(buffers);
      return new DeleteAction(objectManager, null, ByteBufferUtils.getBytes(idLength, buffers));
    }
  };

  private final ObjectManager<ByteBuffer, ?, ?> objectManager;
  private final Compactor compactor;
  private final ByteBuffer id;

  DeleteAction(ObjectManager<ByteBuffer, ?, ?> objectManager, Compactor compactor, ByteBuffer id) {
    this.objectManager = objectManager;
    this.compactor = compactor;
    this.id = id;
  }

  ByteBuffer getId() {
    return id;
  }

  @Override
  public void record(long lsn) {
    objectManager.delete(id);
    compactor.compactNow();
  }

  @Override
  public Set<Long> replay(long lsn) {
    // nothing to do on replay
    return Collections.emptySet();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForId(id).writeLock();
    lock.lock();
    return Collections.singleton(lock);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer buffer = ByteBuffer.allocate(ByteBufferUtils.INT_SIZE);
    buffer.putInt(id.remaining()).flip();
    return new ByteBuffer[] { buffer, id.slice() };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DeleteAction that = (DeleteAction) o;

    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
