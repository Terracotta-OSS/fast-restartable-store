/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
