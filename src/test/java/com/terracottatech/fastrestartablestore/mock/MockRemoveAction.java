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
class MockRemoveAction implements Action<String> {

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
}
