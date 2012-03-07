/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.terracottatech.fastrestartablestore.Compactor;
import com.terracottatech.fastrestartablestore.CompleteKey;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 * 
 * @author cdennis
 */
class MockCompactor<I, K, V> implements Compactor {
  private final TransactionManager txnManager;
  private final ObjectManager<I, K, V> manager;
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
    this.manager = objectManager;
  }

  public void compactNow() {
    try {
      executor.submit(runCompactor).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void compact() {
    CompleteKey<I, K> key = manager.getCompactionKey();
    if (key != null) {
      TransactionHandle txnHandle = txnManager.create();
      txnManager.happened(txnHandle, new MockCompactionAction<I, K, V>(key));
      txnManager.commit(txnHandle);
    }
  }
}
