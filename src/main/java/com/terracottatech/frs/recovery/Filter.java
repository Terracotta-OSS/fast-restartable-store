/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

/**
 *
 * @author cdennis
 */
public interface Filter<T> {
  
  /**
   * Returns {@code true} if the record forms a valid part of the log.
   */
  boolean filter(T element, long lsn);
}
