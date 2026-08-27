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

import java.util.Map;
import java.util.Optional;

public class DefaultEncryptionHandler implements EncryptionHandler {
  
  private final CipherManager cipherManager;
  private final EncryptionActionConverter converter;
  
  public DefaultEncryptionHandler(Configuration config, ActionCodec codec, Map<String, byte[]> tokenToKeyMap, String currentToken) {
    cipherManager = new AESCipherManager(config, tokenToKeyMap, currentToken);
    this.converter = new EncryptionActionConverter(cipherManager);
    EncryptionActions.registerActions(3, codec, cipherManager);
  }
  
  @Override
  public Optional<String> getPreviousToken() {
    return cipherManager.getPreviousToken();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return cipherManager.isUsingEncKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    cipherManager.add(token, key);
  }

  @Override
  public void remove(String token) {
    cipherManager.remove(token);
  }

  @Override
  public Action convert(Action action) {
    return converter.convert(action);
  }
}
