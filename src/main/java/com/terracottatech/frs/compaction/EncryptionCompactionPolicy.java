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

import com.terracottatech.frs.cipher.EncryptionCompletionListener;
import com.terracottatech.frs.object.ObjectManagerEntry;

public class EncryptionCompactionPolicy implements CompactionPolicy {

  private final EncryptionCompletionListener listener;
  private final long maxLsn;

  public EncryptionCompactionPolicy(EncryptionCompletionListener listener, long maxLsn) {
    this.listener = listener;
    this.maxLsn = maxLsn;
  }

  @Override
  public boolean startCompacting() {
    return true;
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    return true;
  }

  @Override
  public void stoppedCompacting(boolean isPaused) {
    if (!isPaused) {
      listener.handleEncryptionCompletionWithNewKey();
    }
  }

  public long getHighestLsnToBeCompacted() {
    return maxLsn;
  }
}
