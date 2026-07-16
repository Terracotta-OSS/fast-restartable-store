/*
 * Copyright IBM Corp. 2024, 2025
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

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.recovery.AbstractFilter;
import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.recovery.RecoveryException;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author cdennis
 */
class MockSkipsFilter extends AbstractFilter<Action> {

  private final Set<Long> skips = new HashSet<Long>();

  public MockSkipsFilter(Filter<Action> next) {
    super(next);
  }
  
  @Override
  public boolean filter(Action action, long lsn, boolean filtered) throws RecoveryException {
    if (skips.remove(lsn)) {
      updateSkips(action);
      return delegate(action, lsn, true);
    } else {
      if (delegate(action, lsn, filtered)) {
        updateSkips(action);
        return true;
      } else {
        return false;
      }
    }
  }

  private void updateSkips(Action action) {
    if (action instanceof InvalidatingAction) {
      skips.addAll(((InvalidatingAction) action).getInvalidatedLsns());
    }
  }
}
