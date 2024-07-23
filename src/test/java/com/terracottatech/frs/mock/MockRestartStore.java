/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.mock;

import com.terracottatech.frs.NotPausedException;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.RestartStoreException;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.Statistics;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.Tuple;
import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.mock.action.MockActionManager;
import com.terracottatech.frs.mock.compaction.MockCompactor;
import com.terracottatech.frs.mock.log.MockLogManager;
import com.terracottatech.frs.mock.object.MockObjectManager;
import com.terracottatech.frs.mock.recovery.MockRecoveryManager;
import com.terracottatech.frs.mock.transaction.MockTransactionManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.recovery.RecoveryException;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.util.NullFuture;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public class MockRestartStore implements RestartStore<Long, String, String> {

  private final TransactionManager txnManager;
  private final ObjectManager<Long, String, String> objManager;
  private final Compactor compactor;

  @Override
  public void shutdown() throws InterruptedException {
    compactor.shutdown();
  }

  @Override
  public Future<Void> startup() {
    compactor.startup();
    return new NullFuture();
  }


  private MockRestartStore(TransactionManager txnManager, ObjectManager<Long, String, String> objManager, Compactor compactor) {
    this.txnManager = txnManager;
    this.objManager = objManager;
    this.compactor = compactor;
  }
  
  public Transaction<Long, String, String> beginTransaction(boolean synchronous) {
    return new MockTransaction(txnManager, objManager);
  }

  @Override
  public Transaction<Long, String, String> beginAutoCommitTransaction(boolean synchronous) {
    throw new UnsupportedOperationException("Mock doesn't support auto-commit transactions");
  }

  public static MockRestartStore create(MockObjectManager<Long, String, String> objManager, IOManager ioManager) throws
          InterruptedException, RecoveryException {
    LogManager logManager = new MockLogManager(ioManager);
    ActionManager actionManager = new MockActionManager(objManager, logManager);
    TransactionManager txnManager = new MockTransactionManager(actionManager);
    Compactor compactor = new MockCompactor<Long, String, String>(txnManager,
                                                                  actionManager,
                                                                  objManager);
    
    RecoveryManager recovery = new MockRecoveryManager(logManager, actionManager);
    recovery.recover();
    
    return new MockRestartStore(txnManager, objManager, compactor);
  }
  
  public void compact() {
    compactor.compactNow();
  }

  @Override
  public Tuple<Long, String, String> get(long marker) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Snapshot snapshot() throws RestartStoreException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statistics getStatistics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<Future<Snapshot>> pause() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resume() throws NotPausedException {
    throw new NotPausedException();
  }

  @Override
  public Future<Future<Void>> freeze() {
    throw new UnsupportedOperationException();
  }

}