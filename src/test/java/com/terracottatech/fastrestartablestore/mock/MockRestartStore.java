/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

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
  
  private MockRestartStore(TransactionManager txnManager) {
    this.txnManager = txnManager;
  }
  
  public Transaction<Long, String, String> beginTransaction() {
    return new MockTransaction(txnManager);
  }

  public static MockRestartStore create(MockObjectManager objManager, IOManager ioManager) {
    ObjectManager txnObjManager = new MockTransactionalObjectManager(objManager);
    LogManager logManager = new MockLogManager(ioManager);
    RecordManager rcdManager = new MockRecordManager(txnObjManager, logManager);
    TransactionManager txnManager = new MockTransactionManager(rcdManager);
    
    RecoveryManager recovery = new MockRecoveryManager(logManager, rcdManager, txnObjManager);
    recovery.recover();
    
    return new MockRestartStore(txnManager);
  }
}
