/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.io.Serializable;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 * 
 * @author cdennis
 */
class MockTransactionalAction implements Action, Serializable {
  private static final long serialVersionUID = 1L;
  private final long id;
  private final Action embedded;

  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }
  
  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return embedded.record(objManager, lsn);
  }

  @Override
  public void replay(ObjectManager<?, ?, ?> objManager, long lsn) {
    embedded.replay(objManager, lsn);
  }

  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

}
