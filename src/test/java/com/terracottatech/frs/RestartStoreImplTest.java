/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionManager;
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
  private RestartStore<Long, ByteBuffer, ByteBuffer> restartStore;
  private ObjectManager<Long, ByteBuffer, ByteBuffer> objectManager;
  private TransactionManager transactionManager;

  @Before
  public void setUp() throws Exception {
    transactionManager = mock(TransactionManager.class);
    objectManager = mock(ObjectManager.class);
    restartStore = new RestartStoreImpl(objectManager, transactionManager);
  }

  @Test
  public void testBegin() throws Exception {
    Transaction<Long, ByteBuffer, ByteBuffer> transaction = restartStore.beginTransaction();
    assertNotNull(transaction);
    verify(transactionManager).begin();
  }

  @Test
  public void testCommit() throws Exception {
    Transaction<Long, ByteBuffer, ByteBuffer> transaction = restartStore.beginTransaction();
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
    Transaction<Long, ByteBuffer, ByteBuffer> transaction = restartStore.beginTransaction();
    transaction.put(1L, newByteBufferWithInt(2), newByteBufferWithInt(3));
    verify(transactionManager).happened(null, new PutAction(objectManager, 1L, newByteBufferWithInt(2), newByteBufferWithInt(3)));
    transaction.commit();
    try {
      transaction.put(4L, newByteBufferWithInt(5), newByteBufferWithInt(6));
      fail("Put on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDelete() throws Exception {
    Transaction<Long, ByteBuffer, ByteBuffer> transaction = restartStore.beginTransaction();
    transaction.delete(1L);
    verify(transactionManager).happened(null, new DeleteAction(objectManager, 1L));
    transaction.commit();
    try {
      transaction.delete(1L);
      fail("Delete on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testRemove() throws Exception {
    Transaction<Long, ByteBuffer, ByteBuffer> transaction = restartStore.beginTransaction();
    transaction.remove(1L, newByteBufferWithInt(2));
    verify(transactionManager).happened(null, new RemoveAction(objectManager, 1L, newByteBufferWithInt(2)));
    transaction.commit();
    try {
      transaction.remove(1L, newByteBufferWithInt(2));
      fail("Remove on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  private ByteBuffer newByteBufferWithInt(int i) {
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(i);
    buffer.flip();
    return buffer;
  }
}
