/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.compaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.CompleteKey;
import com.terracottatech.frs.transaction.TransactionHandle;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.object.ObjectManager;

/**
 * 
 * @author cdennis
 */
public class MockCompactor<I, K, V> implements Compactor {
  private final TransactionManager txnManager;
  private final ObjectManager<I, K, V> objManager;
  private final ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable arg0) {
      Thread t = new Thread(arg0, "CompactorThread");
      t.setDaemon(true);
      return t;
    }
  });

  private final Runnable runCompactor = new Runnable() {
    @Override
    public void run() {
      compact();
    }
  };

  public MockCompactor(TransactionManager txnManager,
      ObjectManager<I, K, V> objectManager) {
    this.txnManager = txnManager;
    this.objManager = objectManager;
  }

  public void compactNow() {
    try {
      executor.submit(runCompactor).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void compact() {
    CompleteKey<I, K> key = objManager.getCompactionKey();
    if (key != null) {
      TransactionHandle txnHandle = txnManager.begin();
      Action compactionAction = new MockCompactionAction<I, K, V>(objManager, key);
      txnManager.happened(txnHandle, compactionAction);
      try {
        txnManager.commit(txnHandle);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
