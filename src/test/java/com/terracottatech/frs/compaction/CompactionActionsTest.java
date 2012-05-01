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
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

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

  private CompactionAction createCompactionAction(int id, int key, Integer value, long invalidates) {
    ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry = new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
            byteBufferWithInt(id), byteBufferWithInt(key), byteBufferWithInt(value), invalidates);
    CompactionAction action =
            new CompactionAction(objectManager, entry);
    action.record(1);
    return action;
  }

  @Test
  public void testCompactionAction() throws Exception {
    CompactionAction validCompaction = createCompactionAction(1, 2, 3, 4);
    assertThat((PutAction) codec.decode(codec.encode(validCompaction)),
               is(new PutAction(objectManager, compactor, byteBufferWithInt(1),
                                byteBufferWithInt(2), byteBufferWithInt(3), 4)));

    validCompaction.record(123);
    validCompaction.updateObjectManager();
    verify(objectManager).updateLsn(entry(1, 2, 3, 4L), 123L);
  }

  private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry(int i, int k, int v, long lsn) {
    return new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
            byteBufferWithInt(i), byteBufferWithInt(k), byteBufferWithInt(v), lsn);
  }
}
