/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.object;

import com.terracottatech.frs.object.CompleteKey;

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
