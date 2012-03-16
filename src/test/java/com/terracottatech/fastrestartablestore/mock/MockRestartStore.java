/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Compactor;
import com.terracottatech.fastrestartablestore.IOManager;
import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.RecoveryManager;
import com.terracottatech.fastrestartablestore.RestartStore;
import com.terracottatech.fastrestartablestore.Transaction;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

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
    RecordManager rcdManager = new MockRecordManager(objManager, logManager);
    TransactionManager txnManager = new MockTransactionManager(rcdManager);
    Compactor compactor = new MockCompactor<Long, String, String>(txnManager, objManager);
    
    RecoveryManager recovery = new MockRecoveryManager(logManager, rcdManager, objManager);
    recovery.recover();
    
    return new MockRestartStore(txnManager, objManager, compactor);
  }
  
  public void compact() {
    compactor.compactNow();
  }
}
