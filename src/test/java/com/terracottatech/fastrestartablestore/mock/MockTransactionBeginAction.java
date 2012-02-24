/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
class MockTransactionBeginAction implements Action {

  private final long id;
  
  public MockTransactionBeginAction(long id) {
    this.id = id;
  }

  public boolean hasKey() {
    return false;
  }

  public Object getKey() {
    throw new UnsupportedOperationException("No keys on txn begin");
  }
  
  public String toString() {
    return "Action: beginTransaction(" + id + ")";
  }
}
