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
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.NullAction;
import com.terracottatech.frs.cipher.EncryptionManager;
import org.junit.Before;
import org.junit.Test;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.flash.ReadManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.NullLogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author tim
 */
public class RestartStoreImplTest {
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>  restartStore;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private ActionManager                                     actionManager;
  private EncryptionManager                                 encryptionManager;
  private ReadManager                                     readManager;
  private Compactor                                         compactor;
  private TransactionManager                                transactionManager;
  private TransactionHandle                                 handle;
  private LogManager                                        logManager;
  private MapActionFactory                                  mapActionFactory;
  private Configuration                                     configuration;
  private Future<Void>                                      syncHappenedFuture;

  @Before
  public void setUp() throws Exception {
    configuration = Configuration.getConfiguration(new File("foo"));
    handle = mock(TransactionHandle.class);
    transactionManager = mock(TransactionManager.class);
    doReturn(handle).when(transactionManager).begin();
    objectManager = mock(ObjectManager.class);
    encryptionManager = mock(EncryptionManager.class);
    actionManager = mock(ActionManager.class);
    syncHappenedFuture = mock(Future.class);
    doReturn(syncHappenedFuture).when(actionManager).syncHappened(any(Action.class));
    readManager = mock(ReadManager.class);
    compactor = mock(Compactor.class);
    logManager = spy(new NullLogManager());
    restartStore = createStore();
    restartStore.startup();
    mapActionFactory = new MapActionFactory(objectManager, compactor);
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore() {
    return new RestartStoreImpl(objectManager, transactionManager, logManager, mock(ActionCodec.class),
                                actionManager, encryptionManager, readManager, compactor, configuration);
  }

  @Test
  public void testIllegalStates() throws Exception {
    restartStore = createStore();
    checkFailBeginTransaction();

    restartStore.startup();

    restartStore.shutdown();
    checkFailBeginTransaction();
  }

  private void checkFailBeginTransaction() throws Exception {
    try {
      restartStore.beginTransaction(true);
      fail();
    } catch (IllegalStateException e) {
    }
    try {
      restartStore.beginAutoCommitTransaction(true);
      fail();
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void testBegin() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction(true);
    assertNotNull(transaction);
    verify(transactionManager).begin();
  }

  @Test
  public void testCommit() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction(true);
    transaction.commit();
    verify(transactionManager).commit(handle, true);
    try {
      transaction.commit();
      fail("Second commit should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testPut() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction(true);
    transaction.put(byteBufferWithInt(1), byteBufferWithInt(2),
                    byteBufferWithInt(3));
    verify(transactionManager).happened(handle, mapActionFactory.put(1, 2, 3));
    transaction.commit();
    try {
      transaction.put(byteBufferWithInt(4), byteBufferWithInt(5),
                      byteBufferWithInt(6));
      fail("Put on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDelete() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction(true);
    transaction.delete(byteBufferWithInt(1));
    verify(transactionManager).happened(handle, mapActionFactory.delete(1));
    transaction.commit();
    try {
      transaction.delete(byteBufferWithInt(1));
      fail("Delete on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testRemove() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction(true);
    transaction.remove(byteBufferWithInt(1), byteBufferWithInt(2));
    verify(transactionManager).happened(handle, mapActionFactory.remove(1, 2));
    transaction.commit();
    try {
      transaction.remove(byteBufferWithInt(1), byteBufferWithInt(2));
      fail("Remove on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testSyncAutoCommitPut() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginAutoCommitTransaction(true);
    transaction.put(byteBufferWithInt(1), byteBufferWithInt(2),
                    byteBufferWithInt(3));
    verify(actionManager).syncHappened(mapActionFactory.put(1, 2, 3));
    verify(syncHappenedFuture).get();
  }

  @Test
  public void testAsyncAutoCommitPut() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginAutoCommitTransaction(false);
    transaction.put(byteBufferWithInt(1), byteBufferWithInt(2),
                    byteBufferWithInt(3));
    verify(actionManager).happened(mapActionFactory.put(1, 2, 3));
    verify(syncHappenedFuture, never()).get();
  }

  @Test
  public void testAsyncAutoCommitRemove() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(false);
    transaction.remove(byteBufferWithInt(1), byteBufferWithInt(15));
    verify(actionManager).happened(mapActionFactory.remove(1, 15));
    verify(syncHappenedFuture, never()).get();
  }

  @Test
  public void testSyncAutoCommitRemove() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.remove(byteBufferWithInt(1), byteBufferWithInt(15));
    verify(actionManager).syncHappened(mapActionFactory.remove(1, 15));
    verify(syncHappenedFuture).get();
  }

  @Test
  public void testAsyncAutoCommitDelete() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(false);
    transaction.delete(byteBufferWithInt(99));
    verify(actionManager).happened(mapActionFactory.delete(99));
    verify(syncHappenedFuture, never()).get();
  }

  @Test
  public void testSyncAutoCommitDelete() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.delete(byteBufferWithInt(99));
    verify(actionManager).syncHappened(mapActionFactory.delete(99));
    verify(syncHappenedFuture).get();
  }

  @Test
  public void testAutoCommitCommit() throws Exception {
    Transaction transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.commit();
    verify(transactionManager, never()).commit(handle, true);
  }

  @Test
  public void testSnapshot() throws Exception {
    restartStore.snapshot();
    verify(logManager).snapshot();
    verify(compactor).pause();
    verify(compactor).unpause();
  }

  @Test
  public void testStatistics() throws Exception {
    restartStore.getStatistics();
    verify(logManager).getIOStatistics();
  }

  @Test
  public void testPauseResume() throws Exception {
    Future<Future<Snapshot>> f = restartStore.pause();
    f.get();

    verify(compactor).pause();
    verify(actionManager).pause();

    restartStore.resume();
    verify(actionManager).resume();
  }

  @Test
  public void testPauseAutoResume() throws Exception {
    Properties properties = new Properties();
    properties.put(FrsProperty.STORE_MAX_PAUSE_TIME_IN_MILLIS.shortName(), Long.toString(20));
    configuration = Configuration.getConfiguration(new File("foo"), properties);
    restartStore = createStore();
    restartStore.startup();

    Future<Future<Snapshot>> f = restartStore.pause();
    f.get();

    verify(compactor).pause();
    verify(actionManager).pause();

    Thread.sleep(200);

    try {
      restartStore.resume();
      fail("Should have auto resumed");
    } catch (NotPausedException e) {
      verify(actionManager).resume();
      verify(compactor).unpause();
    }
  }

  /**
   * handleEncKeyChange() must call {@link Compactor#compactTillLsn} with exactly the LSN
   * that was recorded on the {@link NullAction} barrier by {@link ActionManager#pause(Action)}.
   * This ensures re-encryption covers all records written before the key change.
   */
  @Test
  public void testHandleEncKeyChangeTriggersCompactionTillPauseActionLsn() throws Exception {
    long barrierLsn = 42L;
    // Simulate ActionManager stamping the NullAction with its LSN on pause
    doAnswer(inv -> {
      NullAction nullAction = inv.getArgument(0);
      nullAction.record(barrierLsn);
      return null;
    }).when(actionManager).pause(any(NullAction.class));
    stubCompactTillLsnToComplete(false);

    restartStore.handleEncKeyChange("new-token", CipherHelper.generateNewKey());

    verify(actionManager).pause(any(NullAction.class));
    verify(encryptionManager).add(eq("new-token"), any());
    verify(actionManager).resume();
    verify(compactor).compactTillLsn(eq(barrierLsn), any());
  }

  /**
   * handleEncKeyChange() must call {@link ActionManager#resume()} in its finally block
   * even when an exception is thrown (e.g. invalid Base64 key material), so that the gate
   * is never left closed.
   */
  @Test
  public void testHandleEncKeyChangeAlwaysResumesActionManager() throws Exception {
    // Pass an invalid Base64 string to force an exception inside the try block
    try {
      restartStore.handleEncKeyChange("new-token", "not-valid-base64!!!");
    } catch (IllegalArgumentException e) {
      // expected — Base64.getDecoder().decode() throws on illegal input
    }

    verify(actionManager).resume();
  }

  /**
   * After a successful re-encryption compaction, the registered
   * {@link RestartStore.EncryptionCompletionEvent} listener must be invoked exactly once.
   */
  @Test
  public void testHandleEncKeyChangeNotifiesListenerOnSuccess() throws Exception {
    stubCompactTillLsnToComplete(false);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    Consumer<RestartStore.EncryptionCompletionEvent> listener = encryptionCompletionEvent -> {
      try {
        assertNull(encryptionCompletionEvent.getError());
        assertThat(encryptionCompletionEvent.getExpiredTokens().size(), is(1));
        assertTrue(encryptionCompletionEvent.getExpiredTokens().contains("old-tok"));
        assertThat(encryptionCompletionEvent.getRestartStore(), is(restartStore));
      } catch (AssertionError e) {
        exceptionRef.set(e);
      } finally {
        latch.countDown();
      }
    };
    restartStore.registerEncCompletionListener(listener);

    when(encryptionManager.getPreviousTokens()).thenReturn(Collections.singletonList("old-tok"));
    restartStore.handleEncKeyChange("tok", CipherHelper.generateNewKey());
    latch.await();

    if (exceptionRef.get() != null) {
      throw new AssertionError("Assertion failed in listener", exceptionRef.get());
    }
  }

  @Test
  public void testHandleEncKeyChangeNotifiesListenerOnFailure() throws Exception {
    stubCompactTillLsnToComplete(true);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> exceptionRef = new AtomicReference<>();
    Consumer<RestartStore.EncryptionCompletionEvent> listener = encryptionCompletionEvent -> {
      try {
        assertThat(encryptionCompletionEvent.getError().getMessage(), is("Rewrite failed"));
        assertNull(encryptionCompletionEvent.getExpiredTokens());
        assertThat(encryptionCompletionEvent.getRestartStore(), is(restartStore));
      } catch (AssertionError e) {
        exceptionRef.set(e);
      } finally {
        latch.countDown();
      }
    };
    restartStore.registerEncCompletionListener(listener);
    restartStore.handleEncKeyChange("tok", CipherHelper.generateNewKey());
    latch.await();
    if (exceptionRef.get() != null) {
      throw new AssertionError("Assertion failed in listener", exceptionRef.get());
    }
  }

  private void stubCompactTillLsnToComplete(boolean shouldFail) {
    if (shouldFail) {
      CompletableFuture<Void> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new RuntimeException("Rewrite failed"));
      doReturn(failedFuture).when(compactor).compactTillLsn(anyLong(), any());
    } else {
      doReturn(CompletableFuture.completedFuture(null))
          .when(compactor).compactTillLsn(anyLong(), any());
    }
  }
}
