/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.MapActions;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionActions;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

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
    public long getPreviousLsn() {
      return 0;
    }

    @Override
    public void record(long lsn) {
    }

    @Override
    public void replay(long lsn) {
    }

    @Override
    public Collection<Lock> lock(TransactionLockProvider lockProvider) {
      return null;
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
