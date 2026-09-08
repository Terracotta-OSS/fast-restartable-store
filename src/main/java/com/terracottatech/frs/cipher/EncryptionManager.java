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

import java.util.List;
import java.util.Optional;

public interface EncryptionManager {
  
  String getCurrToken();
  /**
   * Gets the token identifying the previously used encryption key, if any.
   *
   * @return list of previous key tokens
   */
  List<String> getPreviousTokens();

  /**
   * Checks if the encryption key identified by the given token is being used.
   *
   * @param token the token identifying the encryption key to check
   * @return true if the specified encryption key is currently in use, false otherwise
   */
  boolean isUsingEncKey(String token);

  /**
   * Adds a new encryption key with the specified token identifier.
   *
   * @param token the token to identify this encryption key
   * @param key the encryption key bytes to add
   */
  void add(String token, byte[] key);

  /**
   * Removes the encryption key identified by the given tokens.
   *
   * @param tokens list of tokens identifying the encryption key to remove
   */
  void remove(List<String> tokens);
  
  Action convert(Action action);
}
