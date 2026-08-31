/*
 * Copyright IBM Corp. 2024, 2025
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
package com.terracottatech.frs.recovery;

import org.junit.Before;
import org.junit.Test;

import com.terracottatech.frs.Constants;
import com.terracottatech.frs.ExposedDeleteAction;
import com.terracottatech.frs.MapActionFactory;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.cipher.EncryptedAction;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.ExposedTransactionalAction;
import com.terracottatech.frs.transaction.TransactionActionFactory;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.util.TestUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author tim
 */
public class RecoveryManagerImplTest extends AbstractRecoveryManagerImplTest {

  private TransactionActionFactory transactionActionFactory;
  private MapActionFactory mapActionFactory;
  private LogManager logManager;
  private RecoveryManager recoveryManager;

  @Before
  public void setUp() throws Exception {
    ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager = mock(ObjectManager.class);
    transactionActionFactory = new TransactionActionFactory();
    mapActionFactory = new MapActionFactory(objectManager, mock(Compactor.class));
    logManager = newLogManager();
    actionManager = newActionManager();
    recoveryManager = new RecoveryManagerImpl(logManager, actionManager, Configuration.getConfiguration(testFolder.newFolder()));
  }

  @Test
  public void testRecover() throws Exception {
    // Try skipping something...
    logManager.append(record(8, action(false)));
    Action skipper = action(8, true);
    logManager.append(record(9, skipper));

    // Check that an action skipped by one in a transaction is properly skipped
    logManager.append(record(10, action(false)));
    Action validTransactional = action(10, true);
    logManager.append(record(12, transactionActionFactory.transactionalAction(1, validTransactional, true)));
    logManager.append(record(13, transactionActionFactory.transactionCommit(1)));

    // Test a torn transaction
    logManager.append(record(15, transactionActionFactory.transactionalAction(2, action(12, false), true)));

    // Try out a deleted action
    logManager.append(record(16, skipped(mapActionFactory.put(1, 2, 3))));
    logManager.append(record(17, skipped(mapActionFactory.put(1, 3, 5))));
    logManager.append(record(18, skipped(mapActionFactory.put(1, 4, 5))));
    Action checkedPut = spy(mapActionFactory.put(2, 3, 4));
    logManager.append(record(19, checkedPut));
    logManager.append(record(20, mapActionFactory.delete(1)));

    logManager.updateLowestLsn(8);

    recoveryManager.recover();

    verify(skipper).replay(9);
    verify(validTransactional).replay(12);
    verify(checkedPut).replay(19);
  }
  
    @Test
  public void testRecoverZeroItems() throws Exception {

    recoveryManager.recover();

  }
    
    @Test
  public void testRecoverOneItem() throws Exception {
    logManager.append(record(8, action(true)));
    logManager.updateLowestLsn(8);

    recoveryManager.recover();
  }
    
  @Test
  public void testRecoveryError() throws Exception {
    Action errorAction = mock(Action.class);
    doThrow(new AssertionError()).when(errorAction).replay(anyLong());
    logManager.append(record(Constants.FIRST_LSN, errorAction));

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
  }

  @Test
  public void testMissingRecordsOnRecovery() throws Exception {
    logManager.append(record(200, action(true)));
    logManager.updateLowestLsn(Constants.FIRST_LSN);

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
  }
  
  
  @Test
  public void testDisposal() throws Exception {
    TransactionHandle handle = () -> {
      ByteBuffer buffer = ByteBuffer.allocate(8);
      buffer.putLong(1);
      buffer.flip();
      return buffer;
    };
    ExposedDeleteAction delete = mock(ExposedDeleteAction.class);
    when(delete.getId()).thenReturn(TestUtils.byteBufferWithInt(1));
    ExposedDeleteAction wrappedDelete = mock(ExposedDeleteAction.class);
    when(wrappedDelete.getId()).thenReturn(TestUtils.byteBufferWithInt(1));
    PutAction put = mock(PutAction.class);
    PutAction wrappedPut = mock(PutAction.class);
    ExposedTransactionalAction putTransaction = new ExposedTransactionalAction(handle, true, true, wrappedPut, null);
    ExposedTransactionalAction deleteTransaction = new ExposedTransactionalAction(handle, true, true, wrappedDelete, null);
    
    logManager.append(record(200, delete));
    logManager.append(record(201, put));
    logManager.append(record(203, putTransaction));
    logManager.append(record(204, deleteTransaction));
    logManager.updateLowestLsn(Constants.FIRST_LSN);

    try {
      recoveryManager.recover();
      fail();
    } catch (RecoveryException e) {
      // Expected
    }
    verify(put).dispose();
    verify(wrappedPut).dispose();
    verify(delete).dispose();
    verify(wrappedDelete).dispose();
  }

  @Test
  public void testCorrectTcclInReplayThreads() throws Exception {
    ClassLoader loader = mock(ClassLoader.class);
    AtomicReference<ClassLoader> tccl = new AtomicReference<>();
    Action tcclCapture = mock(Action.class);
    doAnswer(inv -> {
      tccl.set(Thread.currentThread().getContextClassLoader());
      return null;
    }).when(tcclCapture).replay(anyLong());

    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(loader);
      //pick up TCCL
      setUp();

      logManager.append(record(Constants.FIRST_LSN, tcclCapture));
      recoveryManager.recover();
    } finally {
      Thread.currentThread().setContextClassLoader(previous);
    }

    assertThat(tccl.get(), sameInstance(loader));
  }

  @Test
  public void testEncryptedActionsAllSameToken() throws Exception {
    // All encrypted actions share the same token → no partial re-encryption
    String token = "token-A";

    logManager.append(record(Constants.FIRST_LSN, encryptedAction(token, true)));
    logManager.append(record(Constants.FIRST_LSN + 1, encryptedAction(token, true)));
    logManager.append(record(Constants.FIRST_LSN + 2, encryptedAction(token, true)));

    recoveryManager.recover((latestEncToken, partialEncWithNewKey, maxLsnForEncStart) -> {
      assertEquals(token, latestEncToken);
      assertFalse(partialEncWithNewKey);
      assertEquals(0L, maxLsnForEncStart);
    });
  }

  @Test
  public void testEncryptedActionsWithTokenChange() throws Exception {
    // RecoveryTestLogManager prepends each record (records.add(0, record)), so
    // the last-appended record is the first iterated.
    //
    // Append order:  FIRST_LSN(tokenA), FIRST_LSN+1(tokenA), FIRST_LSN+2(tokenB)
    // Iteration order: FIRST_LSN+2(tokenB), FIRST_LSN+1(tokenA), FIRST_LSN(tokenA)
    //
    // First record iterated → sets latestEncToken = tokenB.
    // Second record (tokenA) → mismatch: isPartialEnc=true, maxLsnTillReEnc = (FIRST_LSN+1)+1.
    // Third record (tokenA) → guard !isPartialEnc is false, skipped.
    String tokenA = "token-A";
    String tokenB = "token-B";
    long lsnWithMismatch = Constants.FIRST_LSN + 1;

    logManager.append(record(Constants.FIRST_LSN, encryptedAction(tokenA, true)));
    logManager.append(record(lsnWithMismatch, encryptedAction(tokenA, true)));
    logManager.append(record(Constants.FIRST_LSN + 2, encryptedAction(tokenB, true)));

    recoveryManager.recover((latestEncToken, partialEncWithNewKey, maxLsnForEncStart) -> {

      // latestEncToken = tokenB (first EncryptedAction seen during iteration, highest LSN)
      assertEquals(tokenB, latestEncToken);
      assertTrue(partialEncWithNewKey);
      // maxLsnTillReEnc = lsn+1 of the first record whose token differed from latestEncToken
      assertEquals(lsnWithMismatch + 1, maxLsnForEncStart);
    });
  }

  @Test
  public void testNoEncryptedActions() throws Exception {
    // Plain (non-encrypted) actions only → listener called with null token, no partial, maxLsn=0
    logManager.append(record(Constants.FIRST_LSN, action(true)));

    recoveryManager.recover((latestEncToken, partialEncWithNewKey, maxLsnForEncStart) -> {
      assertNull(latestEncToken);
      assertFalse(partialEncWithNewKey);
      assertEquals(0L, maxLsnForEncStart);
    });
  }

  @Test
  public void testMixedEncryptedAndPlainActions() throws Exception {
    // Mix of plain and encrypted actions with one token → no partial re-encryption
    String token = "token-X";

    logManager.append(record(Constants.FIRST_LSN, action(true)));
    logManager.append(record(Constants.FIRST_LSN + 1, encryptedAction(token, true)));
    logManager.append(record(Constants.FIRST_LSN + 2, encryptedAction(token, true)));

    recoveryManager.recover((latestEncToken, partialEncWithNewKey, maxLsnForEncStart) -> {
      assertEquals(token, latestEncToken);
      assertFalse(partialEncWithNewKey);
      assertEquals(0L, maxLsnForEncStart);
    });
  }

  /**
   * An action that also implements {@link EncryptedAction} with a fixed token.
   */
  private interface EncryptedTestAction extends Action, EncryptedAction {
  }

  private Action encryptedAction(String token, boolean shouldReplay) {
    EncryptedTestAction action = mock(EncryptedTestAction.class);
    when(action.getToken()).thenReturn(token);
    if (!shouldReplay) {
      doThrow(new AssertionError("Should not have been executed.")).when(action).replay(anyLong());
    }
    return action;
  }

  private Action skipped(Action action) {
    Action a = spy(action);
    doThrow(new AssertionError("Should not have been executed.")).when(a).replay(anyLong());
    return a;
  }
}
