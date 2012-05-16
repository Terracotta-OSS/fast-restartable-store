/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

/**
 * @author tim
 */
public abstract class AbstractFilter<T> implements Filter<T> {
  private final Filter<T> nextFilter;

  public AbstractFilter(Filter<T> nextFilter) {
    this.nextFilter = nextFilter;
  }

  protected boolean delegate(T element, long lsn, boolean filtered) {
    return nextFilter.filter(element, lsn, filtered);
  }
}
