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
class MockTransactionalAction<K> implements Action<K> {

  private final long id;
  private final Action<K> embedded;
  
  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }

  public boolean hasKey() {
    return embedded.hasKey();
  }

  public K getKey() {
    return embedded.getKey();
  }
  
}
