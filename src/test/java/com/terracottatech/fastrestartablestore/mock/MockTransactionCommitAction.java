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
class MockTransactionCommitAction implements Action {

  private final long id;
  
  public MockTransactionCommitAction(long id) {
    this.id = id;
  }

  public boolean hasKey() {
    return false;
  }

  public Object getKey() {
    throw new UnsupportedOperationException("No keys on txn commit");
  }
    
  public String toString() {
    return "Action: commitTransaction(" + id + ")";
  }
}
