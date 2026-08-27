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
package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.AbstractFilter;
import com.terracottatech.frs.recovery.Filter;

public class EncryptionFilter extends AbstractFilter<Action> {

  private boolean isPartialEnc;
  private boolean endMarkerSeen;
  private boolean latestCompleteEnc;
  private long maxLsnForPartialEnc = Long.MAX_VALUE;

  public EncryptionFilter(Filter<Action> nextFilter) {
    super(nextFilter);
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof EncryptionBeginAction || element instanceof EncryptionEndAction) {
      if (check()) {
        if (element instanceof EncryptionEndAction) {
          endMarkerSeen = true;
        } else {
          if (!endMarkerSeen) {
            isPartialEnc = true;
            maxLsnForPartialEnc = lsn;
          } else {
            latestCompleteEnc = true;
          }
        }
      }
      return false;
    }
    return delegate(element, lsn, filtered);
  }


  public boolean isPartialRecovery() {
    return isPartialEnc;
  }

  public long getMaxLsnForPartialEnc() {
    return maxLsnForPartialEnc;
  }
  
  private boolean check() {
    return !latestCompleteEnc && !isPartialEnc;
  }
}
