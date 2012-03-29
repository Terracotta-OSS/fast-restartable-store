/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.mock.action.MockActionManager;
import com.terracottatech.frs.mock.log.MockLogManager;
import com.terracottatech.frs.mock.object.MockObjectManager;
import com.terracottatech.frs.mock.recovery.MockRecoveryManager;
import com.terracottatech.frs.mock.transaction.MockTransactionManager;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.RestartStore;
import com.terracottatech.frs.Transaction;
import com.terracottatech.frs.transaction.TransactionManager;
import com.terracottatech.frs.mock.compaction.MockCompactor;
import com.terracottatech.frs.object.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockRestartStore implements RestartStore<Long, String, String> {

  private final TransactionManager txnManager;
  private final ObjectManager<Long, String, String> objManager;
  private final Compactor compactor;
  
  private MockRestartStore(TransactionManager txnManager, ObjectManager<Long, String, String> objManager, Compactor compactor) {
    this.txnManager = txnManager;
    this.objManager = objManager;
    this.compactor = compactor;
  }
  
  public Transaction<Long, String, String> beginTransaction() {
    return new MockTransaction(txnManager, objManager);
  }

  public static MockRestartStore create(MockObjectManager<Long, String, String> objManager, IOManager ioManager) {
    LogManager logManager = new MockLogManager(ioManager);
    ActionManager actionManager = new MockActionManager(objManager, logManager);
    TransactionManager txnManager = new MockTransactionManager(actionManager);
    Compactor compactor = new MockCompactor<Long, String, String>(txnManager, objManager);
    
    RecoveryManager recovery = new MockRecoveryManager(logManager, actionManager);
    recovery.recover();
    
    return new MockRestartStore(txnManager, objManager, compactor);
  }
  
  public void compact() {
    compactor.compactNow();
  }
}
