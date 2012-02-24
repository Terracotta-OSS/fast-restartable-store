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
class MockRemoveAction implements Action<String, String>, Serializable {

  private final String key;
  
  public MockRemoveAction(String key) {
    this.key = key;
  }

  public boolean hasKey() {
    return true;
  }

  public String getKey() {
    return key;
  }
  
  public String toString() {
    return "Action: remove(" + key + ")";
  }

  public boolean replay(ObjectManager<String, String> objManager, Set<Long> validTxnIds, long lsn) {
    objManager.replayRemove(key, lsn);
    return true;
  }
}
