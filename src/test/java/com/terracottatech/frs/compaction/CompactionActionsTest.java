/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.object.SimpleObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class CompactionActionsTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec;
  private Compactor compactor;

  @Before
  public void setUp() throws Exception {
    compactor = mock(Compactor.class);
    objectManager = mock(ObjectManager.class);
    codec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(
            objectManager);
    CompactionActions.registerActions(0, codec);
  }

  private TransactionLockProvider lockProvider(boolean lockSuccess) {
    TransactionLockProvider transactionLockProvider = mock(TransactionLockProvider.class);
    Lock lock = mock(Lock.class);
    ReadWriteLock readWriteLock = mock(ReadWriteLock.class);
    doReturn(lock).when(readWriteLock).writeLock();
    doReturn(lockSuccess).when(lock).tryLock();
    doReturn(readWriteLock).when(transactionLockProvider).getLockForKey(anyObject(), anyObject());
    return transactionLockProvider;
  }

  private CompactionAction createCompactionAction(int id, int key, Integer value, long invalidates, boolean valid) {
    ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry = new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
            byteBufferWithInt(id), byteBufferWithInt(key), byteBufferWithInt(value), invalidates);
    CompactionAction action =
            new CompactionAction(objectManager, entry);
    action.lock(lockProvider(valid));
    action.record(1);
    return action;
  }

  @Test
  public void testValidCompactionAction() throws Exception {
    CompactionAction validCompaction = createCompactionAction(1, 2, 3, 4, true);
    assertThat((PutAction) codec.decode(codec.encode(validCompaction)),
               is(new PutAction(objectManager, compactor, byteBufferWithInt(1),
                                byteBufferWithInt(2), byteBufferWithInt(3), 4)));

    validCompaction.record(123);
    validCompaction.updateObjectManager();
    verify(objectManager).updateLsn(entry(1, 2, 3, 4L), 123L);
  }

  @Test
  public void testInvalidCompactionAction() throws Exception {
    CompactionAction invalidCompaction = createCompactionAction(1, 2, 3, 4, false);
    assertThat(codec.decode(codec.encode(invalidCompaction)), is(CompactionAction.NULL_ACTION));
    invalidCompaction.record(123L);
    invalidCompaction.updateObjectManager();
    verify(objectManager, never()).updateLsn(any(ObjectManagerEntry.class), eq(123L));
  }

  private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry(int i, int k, int v, long lsn) {
    return new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
            byteBufferWithInt(i), byteBufferWithInt(k), byteBufferWithInt(v), lsn);
  }
}
