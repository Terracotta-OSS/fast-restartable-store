/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.IOManager;
import com.terracottatech.fastrestartablestore.LogManager;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.RestartStore;
import com.terracottatech.fastrestartablestore.TransactionContext;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
public class MockRestartStore implements RestartStore<String, String> {

  private final TransactionManager<String, String> txnManager;
  
  private MockRestartStore(TransactionManager<String, String> txnManager) {
    this.txnManager = txnManager;
  }
  
  public TransactionContext<String, String> createTransaction() {
    return new MockTransactionContext(txnManager);
  }

  public static MockRestartStore create(ObjectManager<String, String> objManager) {
    IOManager ioManager = new MockIOManager();
    LogManager logManager = new MockLogManager(ioManager);
    RecordManager rcdManager = new MockRecordManager(objManager, logManager);
    TransactionManager<String, String> txnManager = new MockTransactionManager(rcdManager);
    return new MockRestartStore(txnManager);
  }
}
