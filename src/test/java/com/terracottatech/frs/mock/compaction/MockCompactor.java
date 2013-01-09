/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.compaction;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;
import com.terracottatech.frs.transaction.TransactionManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * 
 * @author cdennis
 */
public class MockCompactor<I, K, V> implements Compactor {
  private final TransactionManager txnManager;
  private final ActionManager actionManager;
  private final ObjectManager<I, K, V> objManager;
  private final ExecutorService executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable arg0) {
      Thread t = new Thread(arg0, "CompactorThread");
      t.setDaemon(true);
      return t;
    }
  });

  @Override
  public void startup() {
  }

  @Override
  public void shutdown() throws InterruptedException {
  }

  @Override
  public void generatedGarbage(long lsn) {
  }

  private final Runnable runCompactor = new Runnable() {
    @Override
    public void run() {
      compact();
    }
  };

  public MockCompactor(TransactionManager txnManager,
                       ActionManager actionManager, ObjectManager<I, K, V> objectManager) {
    this.txnManager = txnManager;
    this.actionManager = actionManager;
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
    ObjectManagerEntry<I, K, V> entry = objManager.acquireCompactionEntry(txnManager.getLowestOpenTransactionLsn());
    if (entry != null) {
      try {
        actionManager.happened(new MockCompactionAction<I, K, V>(objManager, entry));
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        objManager.releaseCompactionEntry(entry);
      }
    }
  }

  @Override
  public void pause() {
    throw new UnsupportedOperationException("Implement me!");
  }

  @Override
  public void unpause() {
    throw new UnsupportedOperationException("Implement me!");
  }
}
