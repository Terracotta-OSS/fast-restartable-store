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
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.action.ActionManager;
import com.terracottatech.frs.mock.MockDeleteFilter;
import com.terracottatech.frs.mock.transaction.MockTransactionFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.recovery.RecoveryListener;
import com.terracottatech.frs.recovery.RecoveryManager;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.util.NullFuture;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public class MockRecoveryManager implements RecoveryManager {

  private final LogManager logManager;
  private final ActionManager actionManager;

  public MockRecoveryManager(LogManager logManager, ActionManager actionManager) {
    this.logManager = logManager;
    this.actionManager = actionManager;
  }

  @Override
  public Future<Void> recover(RecoveryListener ... listeners) {
    Iterator<LogRecord> it = logManager.startup();

    Filter<Action> replay = new MockReplayFilter();
    Filter<Action> deleteFilter = new MockDeleteFilter(replay);
    Filter<Action> transactionFilter = new MockTransactionFilter(deleteFilter);
    
    Filter<Action> skipsFilter = new MockSkipsFilter(transactionFilter);
    while (it.hasNext()) {
      LogRecord record = it.next();
      Action action = actionManager.extract(record);
      skipsFilter.filter(action, record.getLsn(), false);
    }

    return new NullFuture();
  }
}
