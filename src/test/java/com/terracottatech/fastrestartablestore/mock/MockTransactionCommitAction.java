/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.io.Serializable;
import java.util.Set;

/**
 *
 * @author cdennis
 */
class MockTransactionCommitAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionCommitAction(long id) {
    this.id = id;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void replay(ObjectManager<?, ?, ?> objManager, long lsn) {
    throw new UnsupportedOperationException();
  }

  public String toString() {
    return "Action: commitTransaction(" + id + ")";
  }

  public long getId() {
   return id;
}

}
