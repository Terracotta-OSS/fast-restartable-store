/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.CompleteKey;

/**
 *
 * @author cdennis
 */
public class MockCompleteKey<I, K> implements CompleteKey<I, K> {

  private final I id;
  private final K key;
  
  public MockCompleteKey(I id, K key) {
    this.id = id;
    this.key = key;
  }
  
  @Override
  public I getId() {
    return id;
  }

  @Override
  public K getKey() {
    return key;
  }
  
}
