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
package com.terracottatech.frs.recovery;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.util.CompressedLongSet;

import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class SkipsFilter extends AbstractFilter<Action> {
  private final long lowestLsn;
  private final Set<Long> skips;

  public SkipsFilter(Filter<Action> nextFilter, long lowestLsn, boolean compressed) {
    super(nextFilter);
    this.lowestLsn  = lowestLsn;
    if (compressed) {
      skips = new CompressedLongSet();
    } else {
      skips = new HashSet<Long>();
    }
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (skips.remove(lsn)) {
      updateSkips(element);
      return delegate(element, lsn, true);
    } else {
      if (delegate(element, lsn, filtered)) {
        updateSkips(element);
        return true;
      } else {
        return false;
      }
    }
  }

  private void updateSkips(Action action) {
    if (action instanceof InvalidatingAction) {
      for (long invalid : ((InvalidatingAction) action).getInvalidatedLsns()) {
        if (invalid >= lowestLsn) {
          skips.add(invalid);
        }
      }
    }
  }
}
