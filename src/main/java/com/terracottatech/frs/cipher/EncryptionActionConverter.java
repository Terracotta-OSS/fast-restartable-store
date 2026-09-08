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

import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.transaction.TransactionalAction;

public class EncryptionActionConverter {
  private final CipherManager cipherManager;

  public EncryptionActionConverter(CipherManager cipherManager) {
    this.cipherManager = cipherManager;
  }

  public Action convert(Action action) {
    if (action instanceof GettableAction && !(action instanceof TransactionalAction)) {
      return new EncryptedGettableAction((GettableAction) action, cipherManager);
    }
    return action;
  }
}
