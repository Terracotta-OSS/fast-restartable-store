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
package com.terracottatech.frs.action;

import com.terracottatech.frs.MapActions;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * @author tim
 */
public class ActionCodecImplTest {
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> actionCodec;

  @Before
  public void setUp() throws Exception {
    objectManager = mock(ObjectManager.class);
    actionCodec = new ActionCodecImpl<ByteBuffer, ByteBuffer, ByteBuffer>(objectManager);

    TransactionActions.registerActions(0, actionCodec);
    MapActions.registerActions(1, actionCodec);
  }

  @Test
  public void testAlreadyRegistered() throws Exception {
    try {
      actionCodec.registerAction(0, 0, BogusAction.class, BogusAction.FACTORY);
      fail("Replacing action registered to id 0 should have failed.");
    } catch (IllegalArgumentException e) {
      // expected
    }

    actionCodec.registerAction(2, 0, BogusAction.class, BogusAction.FACTORY);
    try {
      actionCodec.registerAction(2, 1, BogusAction.class, BogusAction.FACTORY);
      fail("Re-registering BogusAction should have failed.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private static class BogusAction implements Action {
    static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
            mock(ActionFactory.class);

    @Override
    public void record(long lsn) {
    }

    @Override
    public void replay(long lsn) {
    }

    @Override
    public ByteBuffer[] getPayload(ActionCodec codec) {
      return new ByteBuffer[0];
    }
  }

  @Test
  public void testUnknownAction() throws Exception {
    Action someOtherAction = mock(Action.class);
    try {
      actionCodec.encode(someOtherAction);
      fail("Should not be able to encode an unknown action");
    } catch (IllegalArgumentException e){
      // expected
    }

    ByteBuffer[] encoded = new ByteBuffer[] { TestUtils.byteBufferWithInt(8),
                                              TestUtils.byteBufferWithInt(7) };
    try {
      actionCodec.decode(encoded);
      fail("Should not be able to decode an unknown action");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
