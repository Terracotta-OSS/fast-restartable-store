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
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.action.Action;

/**
 * Represents an action performed during the compaction process in the fast-restartable store.
 * <p>
 * CompactionAction extends the {@link Action} interface to provide specialized functionality
 * for compaction operations. Compaction is the process of optimizing storage by removing
 * unnecessary or redundant data and updating Log Sequence Numbers (LSNs) in the ObjectManager.
 * <p>
 * Implementations of this interface are responsible for updating the ObjectManager with new LSNs
 * after the action has been recorded. This update cannot happen during the {@link Action#record(long)}
 * method because the compactor typically holds the segment lock at that time.
 * <p>
 * CompactionActions are created and processed by the {@link Compactor} during its compaction cycle.
 */
public interface CompactionAction extends Action {
  /**
   * Updates the ObjectManager with the new LSN after this compaction action has been recorded.
   * <p>
   * This method is called after the action has been sequenced (via {@link Action#record(long)})
   * but typically before the compaction entry is released. It ensures that the ObjectManager's
   * state is updated to reflect the compaction operation.
   * <p>
   * This method cannot be called during {@link Action#record(long)} because the compactor
   * typically holds the segment lock at that time. Instead, it's called separately after
   * the action has been sequenced.
   */
  void updateObjectManager();
}
