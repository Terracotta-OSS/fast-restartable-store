/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.*;
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
class RemoveAction implements InvalidatingAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              long invalidatedLsn = ByteBufferUtils.getLong(buffers);
              return new SimpleInvalidatingAction(Collections.singleton(invalidatedLsn));
            }
          };

  private final ObjectManager<ByteBuffer, ByteBuffer, ?> objectManager;
  private final Compactor compactor;
  private final ByteBuffer id;
  private final ByteBuffer key;

  private long invalidatedLsn;

  RemoveAction(ObjectManager<ByteBuffer, ByteBuffer, ?> objectManager, Compactor compactor, ByteBuffer id, ByteBuffer key) {
    this.objectManager = objectManager;
    this.compactor = compactor;
    this.id = id;
    this.key = key;
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void record(long lsn) {
    objectManager.remove(id, key);
    if (invalidatedLsn != -1) {
      compactor.generatedGarbage();
    }
  }

  @Override
  public Set<Long> replay(long lsn) {
    // Nothing to remove on replay
    return Collections.emptySet();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForKey(id, key).writeLock();
    lock.lock();
    invalidatedLsn = objectManager.getLsn(id, key);
    return Collections.singleton(lock);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer header = ByteBuffer.allocate(ByteBufferUtils.LONG_SIZE);
    header.putLong(invalidatedLsn).flip();
    return new ByteBuffer[] { header };
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
