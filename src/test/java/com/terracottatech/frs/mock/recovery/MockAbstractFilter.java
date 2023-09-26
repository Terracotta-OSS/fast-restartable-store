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
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.recovery.Filter;

/**
 *
 * @author cdennis
 */
public abstract class MockAbstractFilter<T, U> implements Filter<T> {
  
  private final Filter<U> next;
  
  public MockAbstractFilter(Filter<U> next) {
    this.next = next;
  }

  protected final boolean delegate(T element, long lsn, boolean filtered) {
    return next.filter(convert(element), lsn, filtered);
  }
  
  protected abstract U convert(T element);
}
