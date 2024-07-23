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
package com.terracottatech.frs.recovery;

/**
 *
 * @author cdennis
 */
public interface Filter<T> {
  
  /**
   * Pass the given action through this filter.
   *
   * @param element the Action to be passed through the filters
   * @param lsn lsn for the given action
   * @param filtered whether or not the action has already been filtered by a
   *                 previous filter in the chain.
   * @return {@code true} if the record forms a valid part of the log.
   */
  boolean filter(T element, long lsn, boolean filtered);
}
