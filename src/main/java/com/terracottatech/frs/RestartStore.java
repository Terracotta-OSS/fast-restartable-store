/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

/**
 *
 * @author cdennis
 */
public interface RestartStore<I, K, V> {
  
  Transaction<I, K, V> beginTransaction();

  Transaction<I, K, V> beginAutoCommitTransaction();
}
