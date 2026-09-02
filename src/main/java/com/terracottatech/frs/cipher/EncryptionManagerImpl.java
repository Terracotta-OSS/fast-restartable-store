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

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptionManagerImpl implements EncryptionManager {

  public static final String TOKEN_KEY_DELIMITER = ":";
  public static final String MULTIPLE_TOKEN_KEY_DELIMETER = ",";
  
  private final ActionCodec actionCodec;
  private final Configuration configuration;

  private volatile EncryptionHandler cipherKeyHandler;
  private volatile boolean encryptEnabled = false;

  public EncryptionManagerImpl(Configuration configuration, ActionCodec actionCodec) {
    this.configuration = configuration;
    this.actionCodec = actionCodec;
    boolean encrypted = configuration.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE);
    if (encrypted) {
      String oldTokenAndKeys = configuration.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS);
      String newTokenAndKey = configuration.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY);
      Map<String, byte[]> tokenToKeyMap = new HashMap<>();
      
      if(oldTokenAndKeys != null) {
        String[] oldTokensSplit = oldTokenAndKeys.split(MULTIPLE_TOKEN_KEY_DELIMETER);
        for (int i = 0; i < oldTokensSplit.length; ++i) {
          String[] oldTokenAndKey = oldTokensSplit[i].split(TOKEN_KEY_DELIMITER);
          String oldToken = oldTokenAndKey[0];
          byte[] oldKey = Base64.getDecoder().decode(oldTokenAndKey[1]);
          tokenToKeyMap.put(oldToken, oldKey);
        }
      }
      
      String[] newTokenSplit = newTokenAndKey.split(TOKEN_KEY_DELIMITER);
      String newToken = newTokenSplit[0];
      byte[] newKey = Base64.getDecoder().decode(newTokenSplit[1]);
      tokenToKeyMap.put(newToken, newKey);
      
      cipherKeyHandler = new DefaultEncryptionHandler(actionCodec, tokenToKeyMap, newToken);
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
  public List<String> getPreviousTokens() {
    return cipherKeyHandler.getPreviousTokens();
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
      cipherKeyHandler = new DefaultEncryptionHandler(actionCodec, tokenToKeyMap, token);
    }
  }

  @Override
  public void remove(List<String> tokens) {
    cipherKeyHandler.remove(tokens);
  }

  @Override
  public Action convert(Action action) {
    return cipherKeyHandler.convert(action);
  }
}
