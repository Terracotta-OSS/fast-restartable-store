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

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;

/**
 * @author tim
 */
public class NullTransactionManager implements TransactionManager {

  @Override
  public TransactionHandle begin() {
    return null;
  }

  @Override
  public void commit(TransactionHandle handle, boolean synchronous) throws TransactionException {
  }

  @Override
  public void happened(TransactionHandle handle, Action action) {
  }

  @Override
  public long getLowestOpenTransactionLsn() {
    return 0;
  }
}
