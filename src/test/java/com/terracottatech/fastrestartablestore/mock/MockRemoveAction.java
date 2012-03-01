/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.io.Serializable;

import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
class MockRemoveAction<K> implements Action, Serializable {

  private final K key;
  
  public MockRemoveAction(K key) {
    this.key = key;
  }

  public K getKey() {
    return key;
  }
  
  public String toString() {
    return "Action: remove(" + key + ")";
  }

}
