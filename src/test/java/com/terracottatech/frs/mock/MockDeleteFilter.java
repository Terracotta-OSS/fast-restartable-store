/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.mock.recovery.MockAbstractFilter;
import com.terracottatech.frs.recovery.Filter;

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
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof MockDeleteAction<?>) {
      deletedIds.add(((MockDeleteAction<I>) element).getId());
      return true;
    } else if (element instanceof MockCompleteKeyAction<?, ?> && deletedIds.contains(((MockCompleteKeyAction<I, ?>) element).getId())) {
      return delegate(element, lsn, true);
    } else {
      return delegate(element, lsn, filtered);
    }
  }

  @Override
  protected Action convert(Action element) {
    return element;
  }
  
}
