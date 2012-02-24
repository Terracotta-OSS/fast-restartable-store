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
class MockPutAction implements Action<String, String>, Serializable {

  private final String key;
  private final String value;
  
  public MockPutAction(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public boolean hasKey() {
    return true;
  }

  public String getKey() {
    return key;
  }
  
  public String getValue() {
    return value;
  }
  
  public String toString() {
    return "Action: put(" + key +", " + value + ")";
  }

  public boolean replay(ObjectManager<String, String> objManager, Set<Long> committedAndOpenIds, long lsn) {
    objManager.replayPut(key, value, lsn);
    return true;
  }
}
