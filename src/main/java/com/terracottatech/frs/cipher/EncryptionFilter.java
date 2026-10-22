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
import com.terracottatech.frs.recovery.RecoveryException;

public class EncryptionFilter extends AbstractFilter<Action> {
  private final EncryptionInRecoveryListener listener;
  private boolean isPartialEnc;
  private long maxLsnTillReEnc;
  private String latestEncToken;

  public EncryptionFilter(EncryptionInRecoveryListener listener, Filter<Action> nextFilter) {
    super(nextFilter);
    this.listener = listener;
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) throws RecoveryException {
    if (element instanceof EncryptedAction && !isPartialEnc) {
      EncryptedAction action = (EncryptedAction) element;
      if (latestEncToken == null) {
        latestEncToken = action.getToken();
      } else {
        if (!latestEncToken.equals(action.getToken())) {
          isPartialEnc = true;
          maxLsnTillReEnc = lsn + 1;
        }
      }
    }
    return delegate(element, lsn, filtered);
  }

  @Override
  public void finish() throws RecoveryException {
    super.finish();
    listener.initiateEncryption(latestEncToken, isPartialEnc, maxLsnTillReEnc);
  }
}
