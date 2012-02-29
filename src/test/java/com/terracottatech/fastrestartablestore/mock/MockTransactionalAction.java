/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 *
 * @author cdennis
 */
class MockTransactionalAction implements Action, Serializable {

  private final long id;
  private final Action embedded;
  
  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }
  
  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

  public long getId() {
   return id;
}

public Action getEmbedded() {
   return embedded;
}
  
}
