/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.mock.recovery.MockAbstractFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.mock.MockCompleteKeyAction;
import com.terracottatech.frs.mock.MockDeleteAction;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockDeleteFilter<I> extends MockAbstractFilter<Action, Action> {

  private Set<I> deletedIds = new HashSet<I>();

  public MockDeleteFilter(Filter<Action> next) {
    super(next);
  }
  
  @Override
  public boolean filter(Action element, long lsn) {
    if (element instanceof MockDeleteAction<?>) {
      deletedIds.add(((MockDeleteAction<I>) element).getId());
      return true;
    } else if (element instanceof MockCompleteKeyAction<?, ?> && deletedIds.contains(((MockCompleteKeyAction<I, ?>) element).getId())) {
      return true;
    } else {
      return delegate(element, lsn);
    }
  }

  @Override
  protected Action convert(Action element) {
    return element;
  }
  
}
