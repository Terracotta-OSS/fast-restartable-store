/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.recovery;

/**
 * @author tim
 */
public abstract class AbstractAdaptingFilter<T, U> implements Filter<T> {
  private final Filter<U> nextFilter;

  protected AbstractAdaptingFilter(Filter<U> nextFilter) {
    this.nextFilter = nextFilter;
  }

  protected boolean delegate(T element, long lsn, boolean filtered) {
    return nextFilter.filter(convert(element), lsn, filtered);
  }

  protected abstract U convert(T element);
}
