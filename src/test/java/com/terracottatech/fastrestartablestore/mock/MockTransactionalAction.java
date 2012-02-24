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
class MockTransactionalAction<K, V> implements Action<K, V>, Serializable {

  private final long id;
  private final Action<K, V> embedded;
  
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
  
  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

  public boolean replay(ObjectManager<K, V> objManager, Set<Long> validTxnIds, long lsn) {
    if (validTxnIds.contains(id)) {
      return embedded.replay(objManager, Collections.<Long>emptySet(), lsn);
    } else {
      return false;
    }
  }
}
