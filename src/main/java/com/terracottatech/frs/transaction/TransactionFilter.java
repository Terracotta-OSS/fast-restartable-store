/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.AbstractAdaptingFilter;
import com.terracottatech.frs.recovery.Filter;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class TransactionFilter extends AbstractAdaptingFilter<Action, Action> {
  private final Set<TransactionHandle> openTransactions =
          new HashSet<TransactionHandle>();

  public TransactionFilter(Filter<Action> nextFilter) {
    super(nextFilter);
  }

  @Override
  protected Action convert(Action element) {
    if (element instanceof TransactionalAction) {
      return ((TransactionalAction) element).getAction();
    } else {
      return element;
    }
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof  TransactionAction) {
      TransactionAction transactionAction = (TransactionAction) element;
      boolean replayed = true;


      if (transactionAction.isCommit() && !filtered) {
        openTransactions.add(transactionAction.getHandle());
      }

      if (transactionAction instanceof TransactionalAction) {
        if (openTransactions.contains(transactionAction.getHandle())) {
          replayed = delegate(((TransactionalAction) transactionAction).getAction(), lsn, filtered);
        } else {
          replayed = delegate(((TransactionalAction) transactionAction).getAction(), lsn, true);
        }
      }

      if (transactionAction.isBegin()) {
        openTransactions.remove(transactionAction.getHandle());
      }

      return replayed;
    } else {
      return delegate(element, lsn, filtered);
    }
  }
}
