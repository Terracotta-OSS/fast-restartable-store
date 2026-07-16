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

import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.recovery.RecoveryException;

/**
 *
 * @author cdennis
 */
public abstract class AbstractAdaptingFilter<T, U> implements Filter<T> {
  
  private final Filter<U> nextFilter;
  
  public AbstractAdaptingFilter(Filter<U> nextFilter) {
    this.nextFilter = nextFilter;
  }

  protected final boolean delegate(T element, long lsn, boolean filtered) throws RecoveryException {
    return nextFilter.filter(convert(element), lsn, filtered);
  }

  @Override
  public void finish() throws RecoveryException {
    nextFilter.finish();
  }

  protected abstract U convert(T element);
}
