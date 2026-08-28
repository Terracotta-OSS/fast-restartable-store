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
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EncryptionManagerImpl implements EncryptionManager {

  private final ActionCodec actionCodec;
  private final Configuration configuration;

  private volatile EncryptionHandler cipherKeyHandler;
  private volatile boolean encryptEnabled = false;

  public EncryptionManagerImpl(Configuration configuration, ActionCodec actionCodec) {
    this.configuration = configuration;
    this.actionCodec = actionCodec;
    boolean encrypted = configuration.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE);
    if (encrypted) {
      String oldToken = configuration.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKEN);
      byte[] oldKey = configuration.getByteArray(FrsProperty.STORE_ENCRYPTION_OLD_KEY);
      String newToken = configuration.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN);
      byte[] newKey = configuration.getByteArray(FrsProperty.STORE_ENCRYPTION_NEW_KEY);

      Map<String, byte[]> tokenToKeyMap = new HashMap<>();
      if (oldToken != null && oldKey != null) {
        tokenToKeyMap.put(oldToken, oldKey);
      }
      tokenToKeyMap.put(newToken, newKey);
      cipherKeyHandler = new DefaultEncryptionHandler(configuration, actionCodec, tokenToKeyMap, newToken);
      encryptEnabled = true;
    } else {
      cipherKeyHandler = new NoEncryptionHandler();
    }
  }

  @Override
  public String getCurrToken() {
    return cipherKeyHandler.getCurrToken();
  }

  @Override
  public Optional<String> getPreviousToken() {
    return cipherKeyHandler.getPreviousToken();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return cipherKeyHandler.isUsingEncKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    if (encryptEnabled) {
      cipherKeyHandler.add(token, key);
    } else {
      Map<String, byte[]> tokenToKeyMap = new HashMap<>();
      tokenToKeyMap.put(token, key);
      cipherKeyHandler = new DefaultEncryptionHandler(configuration, actionCodec, tokenToKeyMap, token);
    }
  }

  @Override
  public void remove(String token) {
    cipherKeyHandler.remove(token);
  }

  @Override
  public Action convert(Action action) {
    return cipherKeyHandler.convert(action);
  }
}
