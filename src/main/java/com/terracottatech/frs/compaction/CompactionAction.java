/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.action.NullAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import static com.terracottatech.frs.util.ByteBufferUtils.INT_SIZE;

/**
 * @author tim
 */
class CompactionAction extends PutAction {
  static final Action NULL_ACTION = new NullAction();
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              int valid = ByteBufferUtils.getInt(buffers);
              if (valid == 0) {
                return NULL_ACTION;
              } else {
                return PutAction.FACTORY.create(objectManager, codec, buffers);
              }
            }
          };

  private final ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry;
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;

  private Long lsn;
  private boolean valid;

  CompactionAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
    super(objectManager, null, entry.getId(), entry.getKey(), entry.getValue(), entry.getLsn());
    this.objectManager = objectManager;
    this.entry = entry;
  }

  @Override
  public void record(long lsn) {
    synchronized (this) {
      this.lsn = lsn;
      notifyAll();
    }
  }

  synchronized void updateObjectManager() throws InterruptedException {
    while (lsn == null) {
        wait();
    }
    if (valid) {
      objectManager.updateLsn(entry, lsn);
    }
  }

  @Override
  public Set<Long> replay(long lsn) {
    throw new UnsupportedOperationException("Compaction actions can't be replayed.");
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider lockProvider) {
    Lock lock = lockProvider.getLockForKey(getId(), getKey()).writeLock();
    valid = lock.tryLock();
    if (valid) {
      return Collections.singleton(lock);
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer validityHeader = ByteBuffer.allocate(INT_SIZE);
    if (valid) {
      validityHeader.putInt(1).flip();
      return ByteBufferUtils.concatenate(validityHeader, super.getPayload(codec));
    } else {
      validityHeader.putInt(0).flip();
      return new ByteBuffer[]{ validityHeader };
    }
  }
}
