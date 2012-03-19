/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Filter;
import com.terracottatech.fastrestartablestore.messages.Action;
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
