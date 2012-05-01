/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
import com.terracottatech.frs.action.SimpleInvalidatingAction;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
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
    assertThat(actionCodec.decode(actionCodec.encode(action)), is(action));
  }

  @Test
  public void testPutAction() throws Exception {
    Action put =
            new PutAction(objectManager, compactor, TestUtils.byteBufferWithInt(0),
                          TestUtils.byteBufferWithInt(1), TestUtils.byteBufferWithInt(2));
    checkEncodeDecode(put);
  }

  @Test
  public void testDeleteAction() throws Exception {
    Action delete = new DeleteAction(objectManager, compactor, TestUtils.byteBufferWithInt(1));
    checkEncodeDecode(delete);
  }

  @Test
  public void testRemove() throws Exception {
    Action remove = new RemoveAction(objectManager, compactor, TestUtils.byteBufferWithInt(2),
                                     TestUtils.byteBufferWithInt(10));
    Action decoded = new SimpleInvalidatingAction(Collections.singleton(0L));
    assertThat(actionCodec.decode(actionCodec.encode(remove)), is(decoded));
  }

}
