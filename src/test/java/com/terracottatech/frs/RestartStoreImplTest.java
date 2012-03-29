/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.transaction.TransactionManager;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author tim
 */
public class RestartStoreImplTest {
  private RestartStore<Long, String, String> restartStore;
  private ObjectManager<Long, String, String> objectManager;
  private TransactionManager transactionManager;

  @Before
  public void setUp() throws Exception {
    transactionManager = mock(TransactionManager.class);
    objectManager = mock(ObjectManager.class);
    restartStore = new RestartStoreImpl<Long, String, String>(objectManager, transactionManager);
  }

  @Test
  public void testBegin() throws Exception {
    Transaction<Long, String, String> transaction = restartStore.beginTransaction();
    assertNotNull(transaction);
    verify(transactionManager).begin();
  }

  @Test
  public void testCommit() throws Exception {
    Transaction<Long, String, String> transaction = restartStore.beginTransaction();
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
    Transaction<Long, String, String> transaction = restartStore.beginTransaction();
    transaction.put(1L, "2", "3");
    verify(transactionManager).happened(null, new PutAction<Long, String, String>(objectManager, 1L, "2", "3"));
    transaction.commit();
    try {
      transaction.put(4L, "5", "6");
      fail("Put on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }

  @Test
  public void testDelete() throws Exception {
    Transaction<Long, String, String> transaction = restartStore.beginTransaction();
    transaction.delete(1L);
    verify(transactionManager).happened(null, new DeleteAction<Long>(objectManager, 1L));
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
    Transaction<Long, String, String> transaction = restartStore.beginTransaction();
    transaction.remove(1L, "2");
    verify(transactionManager).happened(null, new RemoveAction<Long, String>(objectManager, 1L, "2"));
    transaction.commit();
    try {
      transaction.remove(1L, "2");
      fail("Remove on a committed transaction should have thrown.");
    } catch (IllegalStateException e) {
      // Expected
    }
  }
}
