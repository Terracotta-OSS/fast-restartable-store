/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.recovery.Filter;

/**
 *
 * @author cdennis
 */
public abstract class MockAbstractFilter<T, U> implements Filter<T> {
  
  private final Filter<U> next;
  
  public MockAbstractFilter(Filter<U> next) {
    this.next = next;
  }

  protected final boolean delegate(T element, long lsn) {
    return next.filter(convert(element), lsn);
  }
  
  protected abstract U convert(T element);
}
