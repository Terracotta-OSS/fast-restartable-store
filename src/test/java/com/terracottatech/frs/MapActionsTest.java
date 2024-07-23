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
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.action.SimpleInvalidatingAction;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static junit.framework.Assert.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class MapActionsTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer>   actionCodec;
  private Compactor compactor;

  @Before
  public void setUp() throws Exception {
    objectManager = mock(ObjectManager.class);
    actionCodec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(objectManager);
    compactor = mock(Compactor.class);

    TransactionActions.registerActions(0, actionCodec);
    MapActions.registerActions(1, actionCodec);
  }

  private void checkEncodeDecode(Action action) throws Exception {
    Action decoded = actionCodec.decode(actionCodec.encode(action));
    assertThat(decoded, is(action));
  }

  @Test
  public void testPutAction() throws Exception {
    Action put =
            new PutAction(objectManager, compactor, byteBufferWithInt(0),
                          byteBufferWithInt(1), byteBufferWithInt(2), false);
    checkEncodeDecode(put);
  }

  @Test
  public void testRecoveryModePut() throws Exception {
    doReturn(-1L).when(objectManager).getLsn(byteBufferWithInt(0), byteBufferWithInt(1));
    try {
      new PutAction(objectManager, compactor, byteBufferWithInt(0), byteBufferWithInt(1),
                    byteBufferWithInt(2), true);
      fail();
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDeleteAction() throws Exception {
    Action delete = new DeleteAction(objectManager, compactor, byteBufferWithInt(1), false);
    checkEncodeDecode(delete);
  }

  @Test
  public void testRemove() throws Exception {
    Action remove = new RemoveAction(objectManager, compactor, byteBufferWithInt(2),
                                     byteBufferWithInt(10), false);
    Action decoded = new SimpleInvalidatingAction(Collections.singleton(0L));
    assertThat(actionCodec.decode(actionCodec.encode(remove)), is(decoded));
  }

}
