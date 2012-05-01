/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.action.InvalidatingAction;
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
public class PutAction implements InvalidatingAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              int idLength = ByteBufferUtils.getInt(buffers);
              int keyLength = ByteBufferUtils.getInt(buffers);
              int valueLength = ByteBufferUtils.getInt(buffers);
              long invalidatedLsn = ByteBufferUtils.getLong(buffers);
              ByteBuffer id = ByteBufferUtils.getBytes(idLength, buffers);
              ByteBuffer key = ByteBufferUtils.getBytes(keyLength, buffers);
              ByteBuffer value = ByteBufferUtils.getBytes(valueLength, buffers);
              return new PutAction(objectManager, null, id, key, value, invalidatedLsn);
            }
          };

  private static final int HEADER_SIZE =
          ByteBufferUtils.INT_SIZE * 3 + ByteBufferUtils.LONG_SIZE;

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final ByteBuffer                                        id;
  private final ByteBuffer                                        key;
  private final ByteBuffer                                        value;
  private final Compactor                                         compactor;

  private long                                                    invalidatedLsn;

  protected PutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, Compactor compactor, ByteBuffer id,
            ByteBuffer key, ByteBuffer value) {
    this(objectManager, compactor, id, key, value, -1L);
  }

  public PutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, Compactor compactor, ByteBuffer id,
                    ByteBuffer key, ByteBuffer value, long invalidatedLsn) {
    this.objectManager = objectManager;
    this.compactor = compactor;
    this.id = id;
    this.key = key;
    this.value = value;
    this.invalidatedLsn = invalidatedLsn;
  }

  protected ByteBuffer getId() {
    return id;
  }

  protected ByteBuffer getKey() {
    return key;
  }

  protected ByteBuffer getValue() {
    return value;
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void record(long lsn) {
    objectManager.put(getId(), getKey(), getValue(), lsn);
    if (invalidatedLsn != -1) {
      compactor.generatedGarbage();
    }
  }

  @Override
  public Set<Long> replay(long lsn) {
    return objectManager.replayPut(getId(), getKey(), getValue(), lsn);
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
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    header.putInt(id.remaining());
    header.putInt(key.remaining());
    header.putInt(value.remaining());
    header.putLong(invalidatedLsn).flip();
    return new ByteBuffer[]{header, id.slice(), key.slice(), value.slice()};
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PutAction putAction = (PutAction) o;

    return id.equals(putAction.id) && key.equals(putAction.key) && value.equals(
            putAction.value) && invalidatedLsn == putAction.invalidatedLsn;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
