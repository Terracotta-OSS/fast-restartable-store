/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
