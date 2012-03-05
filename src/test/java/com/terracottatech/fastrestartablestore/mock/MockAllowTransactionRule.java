/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.FilterRule;
import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
class MockAllowTransactionRule implements FilterRule {

  private final long id;
  
  public MockAllowTransactionRule(long id) {
    this.id = id;
  }

  @Override
  public boolean disallows(Action action) {
    return false;
  }

  @Override
  public boolean allows(Action action) {
    if (action instanceof MockTransactionalAction) {
      if (((MockTransactionalAction) action).getId() == id) {
        return true;
      }
    }
    return false;
  }
  
  @Override
  public boolean equals(Object o) {
    return (o instanceof MockAllowTransactionRule) && ((MockAllowTransactionRule) o).id == id;
  }
}
