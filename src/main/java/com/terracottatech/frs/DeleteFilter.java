/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.AbstractFilter;
import com.terracottatech.frs.recovery.Filter;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class DeleteFilter extends AbstractFilter<Action> {
  private final Set<ByteBuffer> deleted = new HashSet<ByteBuffer>();

  public DeleteFilter(Filter<Action> nextFilter) {
    super(nextFilter);
  }

  @Override
  public boolean filter(Action element, long lsn) {
    if (element instanceof DeleteAction) {
      deleted.add(((DeleteAction) element).getId());
      return true;
    } else if (element instanceof PutAction) {
      if (!deleted.contains(((PutAction) element).getId())) {
        return delegate(element, lsn);
      } else {
        return true;
      }
    }
    return delegate(element, lsn);
  }
}