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
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.mock.recovery.MockAbstractFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.action.Action;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockTransactionFilter extends MockAbstractFilter<Action, Action> {

  private final Set<Long> validTransactions = new HashSet<Long>();

  public MockTransactionFilter(Filter<Action> next) {
    super(next);
  }
  
  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof MockTransactionCommitAction) {
      validTransactions.add(((MockTransactionCommitAction) element).getId());
      return true;
    } else if (element instanceof MockTransactionBeginAction) {
      validTransactions.remove(((MockTransactionBeginAction) element).getId());
      return true;
    } else if (element instanceof MockTransactionalAction && !validTransactions.contains(((MockTransactionalAction) element).getId())) {
      return delegate(element, lsn, true);
    } else {
      return delegate(element, lsn, filtered);
    }
  }

  @Override
  protected Action convert(Action element) {
    if (element instanceof MockTransactionalAction) {
      return ((MockTransactionalAction) element).getEmbeddedAction();
    } else {
      return element;
    }
  }
  
}
