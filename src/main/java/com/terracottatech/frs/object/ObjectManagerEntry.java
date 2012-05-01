/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 * @author tim
 */
public interface ObjectManagerEntry<I, K, V> {
  I getId();

  K getKey();

  V getValue();

  long getLsn();
}
