/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author Chris Dennis
 */
public class HeapObjectManagerTest extends ObjectManagerTest {

  @Override
  protected <I, K, V> ObjectManager<I, K, V> createObjectManager() {
    return new HeapObjectManager<I, K, V>(1);
  }
}
