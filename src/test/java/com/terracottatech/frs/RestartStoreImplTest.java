/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.log.NullLogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * @author tim
 */
public class RestartStoreImplTest {
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>  restartStore;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private Compactor                                         compactor;
  private TransactionManager                                transactionManager;
  private TransactionHandle                                 handle;
  private LogManager logManager;
  private MapActionFactory mapActionFactory;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    configuration = Configuration.getConfiguration(new File("foo"));
    handle = mock(TransactionHandle.class);
    transactionManager = mock(TransactionManager.class);
    doReturn(handle).when(transactionManager).begin();
    objectManager = mock(ObjectManager.class);
    compactor = mock(Compactor.class);
    logManager = spy(new NullLogManager());
    restartStore = createStore();
    restartStore.startup();
    mapActionFactory = new MapActionFactory(objectManager, compactor);
  }

  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> createStore() {
    return new RestartStoreImpl(objectManager, transactionManager, logManager,
                                mock(ActionManager.class), compactor, configuration);
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
    } catch (IllegalStateException e) {}
    try {
      restartStore.beginAutoCommitTransaction(true);
      fail();
    } catch (IllegalStateException e) {}
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
  public void testAutoCommitPut() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginAutoCommitTransaction(true);
    transaction.put(byteBufferWithInt(1), byteBufferWithInt(2),
                    byteBufferWithInt(3));
    verify(transactionManager).happened(mapActionFactory.put(1, 2, 3));
  }

  @Test
  public void testAutoCommitRemove() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.remove(byteBufferWithInt(1), byteBufferWithInt(15));
    verify(transactionManager).happened(mapActionFactory.remove(1, 15));
  }

  @Test
  public void testAutoCommitDelete() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer>
            transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.delete(byteBufferWithInt(99));
    verify(transactionManager).happened(mapActionFactory.delete(99));
  }

  @Test
  public void testAutoCommitCommit() throws Exception {
    Transaction transaction = restartStore.beginAutoCommitTransaction(true);
    transaction.commit();
    verify(transactionManager, never()).commit(handle, true);
  }
}
