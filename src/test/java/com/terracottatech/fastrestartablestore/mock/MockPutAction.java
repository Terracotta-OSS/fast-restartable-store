/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import java.io.Serializable;

/**
 *
 * @author cdennis
 */
class MockPutAction<K, V> implements Action, Serializable {

   private static final long serialVersionUID = -696424493751601762L;
   
private final K key;
  private final V value;
  
  public MockPutAction(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public boolean hasKey() {
    return true;
  }

  public K getKey() {
    return key;
  }
  
  public V getValue() {
    return value;
  }
  
  public String toString() {
    return "Action: put(" + key +", " + value + ")";
  }
}
