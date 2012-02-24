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
class MockPutAction implements Action<String> {

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
}
