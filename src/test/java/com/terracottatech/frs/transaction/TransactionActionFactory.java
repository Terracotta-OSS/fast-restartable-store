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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

/**
 * @author tim
 */
public class TransactionActionFactory {
  private static final TransactionLSNCallback NULL_CALLBACK = new TransactionLSNCallback() {
    @Override
    public void setLsn(long lsn) {
    }
  };

  public TransactionHandle transactionHandle(long id) {
    return new TransactionHandleImpl(id);
  }

  public Action transactionCommit(long id) {
    return new TransactionCommitAction(transactionHandle(id), false);
  }

  public Action transactionalAction(long id, Action action, boolean begin) {
    return new TransactionalAction(transactionHandle(id), begin, false, action, NULL_CALLBACK);
  }
}
