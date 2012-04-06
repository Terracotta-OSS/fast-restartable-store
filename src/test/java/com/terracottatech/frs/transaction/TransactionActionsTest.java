/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.MapActions;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionCodecImpl;
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
  private ObjectManager<Long, ByteBuffer, ByteBuffer> objectManager;
  private ActionCodec                                 actionCodec;

  @Before
  public void setUp() throws Exception {
    objectManager = mock(ObjectManager.class);
    actionCodec = new ActionCodecImpl(objectManager);

    TransactionActions.registerActions(0, actionCodec);
    MapActions.registerActions(1, actionCodec);
  }

  private void checkEncodeDecode(Action action) throws Exception {
    assertThat(actionCodec.decode(actionCodec.encode(action)), is(action));
  }

  @Test
  public void testTransactionBegin() throws Exception {
    Action begin = new TransactionBeginAction(new TransactionHandleImpl(10L));
    checkEncodeDecode(begin);
  }

  @Test
  public void testTransactionCommit() throws Exception {
    Action commit = new TransactionCommitAction(new TransactionHandleImpl(11L));
    checkEncodeDecode(commit);
  }

  @Test
  public void testTransactionalAction() throws Exception {
    Action txnBegin = new TransactionBeginAction(new TransactionHandleImpl(1L));
    Action txn = new TransactionalAction(new TransactionHandleImpl(3L), txnBegin);

    ByteBuffer[] encoded = actionCodec.encode(txn);
    assertThat(actionCodec.decode(encoded), is(txn));
  }
}
