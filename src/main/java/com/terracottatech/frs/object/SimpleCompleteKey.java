/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author Chris Dennis
 */
public class SimpleCompleteKey<I, K> implements CompleteKey<I, K> {

  private final I identifer;
  private final K key;

  public SimpleCompleteKey(I identifer, K key) {
    this.identifer = identifer;
    this.key = key;
  }

  @Override
  public I getId() {
    return identifer;
  }

  @Override
  public K getKey() {
    return key;
  }
  
  public boolean equals(Object o) {
    if (o instanceof CompleteKey<?, ?>) {
      CompleteKey<?, ?> key = (CompleteKey<?, ?>) o;
      return getId().equals(key.getId()) && getKey().equals(key.getKey());
    } else {
      return false;
    }
  }
}
