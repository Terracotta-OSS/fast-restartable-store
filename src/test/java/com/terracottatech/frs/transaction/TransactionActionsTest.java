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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.MapActionFactory;
import com.terracottatech.frs.MapActions;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class TransactionActionsTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer>   actionCodec;
  private TransactionLSNCallback callback;
  private MapActionFactory mapActionFactory;

  @Before
  public void setUp() throws Exception {
    objectManager = mock(ObjectManager.class);
    actionCodec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(objectManager);
    callback = mock(TransactionLSNCallback.class);
    mapActionFactory = new MapActionFactory(objectManager, mock(Compactor.class));
    TransactionActions.registerActions(0, actionCodec);
    MapActions.registerActions(1, actionCodec);
  }

  private void checkEncodeDecode(Action action) throws Exception {
    assertThat(actionCodec.decode(actionCodec.encode(action)), is(action));
  }

  @Test
  public void testTransactionCommit() throws Exception {
    Action commit = new TransactionCommitAction(new TransactionHandleImpl(11L), false);
    checkEncodeDecode(commit);
  }

  @Test
  public void testTransactionalAction() throws Exception {
    Action txn = new TransactionalAction(new TransactionHandleImpl(3L),false, false, mapActionFactory.put(1, 2, 3), callback);

    ByteBuffer[] encoded = actionCodec.encode(txn);
    assertThat(actionCodec.decode(encoded), is(txn));

    txn = new TransactionalAction(new TransactionHandleImpl(3L),true, false, mapActionFactory.put(1, 2, 3), callback);

    encoded = actionCodec.encode(txn);

    TransactionAction decoded = (TransactionAction) actionCodec.decode(encoded);
    assertThat(decoded, is(txn));
    assertThat(decoded.getHandle(), is((TransactionHandle) new TransactionHandleImpl(3L)));
    assertThat(decoded.isBegin(), is(true));
  }
}
