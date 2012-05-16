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
   * Pass the given action through this filter.
   *
   * @param element the Action to be passed through the filters
   * @param lsn lsn for the given action
   * @param filtered whether or not the action has already been filtered by a
   *                 previous filter in the chain.
   * @return {@code true} if the record forms a valid part of the log.
   */
  boolean filter(T element, long lsn, boolean filtered);
}
