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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * {@link CipherManager} implementation that manages AES secret keys and
 * delegates all algorithm-specific cipher operations to a {@link CipherAlgorithmDelegate}.
 * By default an {@link AESGCMCipherDelegate} (AES/GCM/NoPadding) is used.
 */
public class AESCipherManager implements CipherManager {

  private final Map<String, SecretKey> tokenToKey = new ConcurrentHashMap<>();
  private volatile SecretKey currentSecretKey;
  private volatile String currentToken;

  private volatile CipherAlgorithmDelegate delegate;

  public AESCipherManager(Map<String, byte[]> tokenToKeyMap, String currentToken) {
    tokenToKeyMap.forEach((token, keyBytes) ->
        tokenToKey.put(token, new SecretKeySpec(keyBytes, "AES")));
    currentSecretKey = tokenToKey.get(currentToken);
    this.currentToken = currentToken;
    this.delegate = new AESGCMCipherDelegate();
  }

  @Override
  public ByteBuffer generateInitializationVector() {
    return delegate.generateInitializationVector();
  }

  @Override
  public ByteBuffer[] encrypt(ByteBuffer[] input, ByteBuffer initializationVector) {
    return delegate.encrypt(input, currentSecretKey, initializationVector);
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer cipherBuffer, ByteBuffer ivBuffer, String token) {
    SecretKey secretKey = tokenToKey.get(token);
    if (secretKey == null) {
      throw new AssertionError(String.format("Invalid token: %s", token));
    }
    return delegate.decrypt(cipherBuffer, secretKey, ivBuffer);
  }

  @Override
  public String getCurrentToken() {
    return currentToken;
  }

  @Override
  public List<String> getPreviousTokens() {
    return tokenToKey.keySet().stream().filter(k -> !k.equals(getCurrentToken())).collect(Collectors.toList());
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return tokenToKey.containsKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    tokenToKey.put(token, new SecretKeySpec(key, "AES"));
    currentSecretKey = tokenToKey.get(token);
    currentToken = token;
  }

  @Override
  public void remove(List<String> tokens) {
    tokens.stream().forEach(t -> tokenToKey.remove(t));
  }
}
