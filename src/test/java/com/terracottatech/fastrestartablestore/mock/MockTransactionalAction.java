/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 * 
 * @author cdennis
 */
class MockTransactionalAction implements MockAction, Serializable {
  private static final long serialVersionUID = 1L;
  private final long id;
  private final Action embedded;

  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (embedded instanceof MockAction) {
      ((MockAction) embedded).setObjectManager(objManager);
    }
  }

  @Override
  public long getPreviousLsn() {
    return embedded.getPreviousLsn();
  }

  @Override
  public void record(long lsn) {
    embedded.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

  long getId() {
    return id;
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    return embedded.lock(locks);
  }

  Action getEmbeddedAction() {
    return embedded;
  }
}
