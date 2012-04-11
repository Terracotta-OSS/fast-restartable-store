/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author tim
 */
public class RestartStoreImplTest {
  private RestartStore<ByteBuffer, ByteBuffer, ByteBuffer>  restartStore;
  private ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private TransactionManager                                transactionManager;

  @Before
  public void setUp() throws Exception {
    transactionManager = mock(TransactionManager.class);
    objectManager = mock(ObjectManager.class);
    restartStore = new RestartStoreImpl(objectManager, transactionManager);
  }

  @Test
  public void testBegin() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction();
    assertNotNull(transaction);
    verify(transactionManager).begin();
  }

  @Test
  public void testCommit() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction();
    transaction.commit();
    verify(transactionManager).commit(null);
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
            restartStore.beginTransaction();
    transaction.put(TestUtils.byteBufferWithInt(1), TestUtils.byteBufferWithInt(2),
                    TestUtils.byteBufferWithInt(3));
    verify(transactionManager).happened(null, new PutAction(objectManager,
                                                            TestUtils.byteBufferWithInt(1),
                                                            TestUtils.byteBufferWithInt(2),
                                                            TestUtils.byteBufferWithInt(3)));
    transaction.commit();
    try {
      transaction.put(TestUtils.byteBufferWithInt(4), TestUtils.byteBufferWithInt(5),
                      TestUtils.byteBufferWithInt(6));
      fail("Put on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDelete() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction();
    transaction.delete(TestUtils.byteBufferWithInt(1));
    verify(transactionManager).happened(null, new DeleteAction(objectManager,
                                                               TestUtils.byteBufferWithInt(1)));
    transaction.commit();
    try {
      transaction.delete(TestUtils.byteBufferWithInt(1));
      fail("Delete on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testRemove() throws Exception {
    Transaction<ByteBuffer, ByteBuffer, ByteBuffer> transaction =
            restartStore.beginTransaction();
    transaction.remove(TestUtils.byteBufferWithInt(1), TestUtils.byteBufferWithInt(2));
    verify(transactionManager).happened(null, new RemoveAction(objectManager,
                                                               TestUtils.byteBufferWithInt(1),
                                                               TestUtils.byteBufferWithInt(2)));
    transaction.commit();
    try {
      transaction.remove(TestUtils.byteBufferWithInt(1), TestUtils.byteBufferWithInt(2));
      fail("Remove on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }
}
