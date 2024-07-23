/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.MapActionFactory;
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
  private MapActionFactory mapActionFactory;

  @Before
  public void setUp() throws Exception {
    compactor = mock(Compactor.class);
    objectManager = mock(ObjectManager.class);
    codec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(
            objectManager);
    CompactionActions.registerActions(0, codec);
    mapActionFactory = new MapActionFactory(objectManager, compactor);
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
    assertThat(codec.decode(codec.encode(validCompaction)),
               is(mapActionFactory.put(1, 2, 3, 4L)));

    validCompaction.record(123);
    validCompaction.updateObjectManager();
    verify(objectManager).updateLsn(entry(1, 2, 3, 4L), 123L);
  }

  private ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry(int i, int k, int v, long lsn) {
    return new SimpleObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer>(
            byteBufferWithInt(i), byteBufferWithInt(k), byteBufferWithInt(v), lsn);
  }
}
