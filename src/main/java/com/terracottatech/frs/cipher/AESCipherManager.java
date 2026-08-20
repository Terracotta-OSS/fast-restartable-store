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
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidParameterSpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

public class AESCipherManager implements CipherManager {

  private final String algorithm;

  private final Map<String, SecretKey> tokenToKey = new ConcurrentHashMap<>();
  private volatile SecretKey currentSecretKey;
  private volatile String currentToken;

  public AESCipherManager(Configuration config, Map<String, byte[]> tokenToKeyMap, String currentToken) {
    algorithm = config.getString(FrsProperty.STORE_ENCRYPTION_ALGORITHM);
    tokenToKeyMap.entrySet().stream().forEach(entry ->
      tokenToKey.put(entry.getKey(), new SecretKeySpec(entry.getValue(), "AES"))
    );
    currentSecretKey = tokenToKey.get(currentToken);
    this.currentToken = currentToken;
  }

  private Cipher getCipher() {
    try {
      return Cipher.getInstance(algorithm);
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException("invalid cipher algorithm", ex);
    } catch (NoSuchPaddingException ex) {
      throw new IllegalArgumentException("padding mechanism not available", ex);
    }
  }

  @Override
  public ByteBuffer generateInitializationVector() {
    Cipher cipher = getCipher();
    AlgorithmParameters params = cipher.getParameters();
    byte[] iv;
    try {
      iv = params.getParameterSpec(IvParameterSpec.class).getIV();
    } catch (InvalidParameterSpecException ex) {
      iv = new byte[16];
      new SecureRandom().nextBytes(iv);
    }
    return ByteBuffer.wrap(iv);
  }

  private Cipher getCipher(SecretKey secretKey, int operation, ByteBuffer ivBuffer) {
    Cipher cipher = getCipher();
    byte[] iv;
    if (ivBuffer.hasArray()) {
      iv = ivBuffer.array();
    } else {
      iv = new byte[ivBuffer.remaining()];
      ivBuffer.get(iv);
    }

    try {
      cipher.init(operation, secretKey, new IvParameterSpec(iv));
      return cipher;
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException("invalid cipher key", e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("invalid parameter for cipher algorithm", e);
    }
  }

  @Override
  public ByteBuffer[] encrypt(ByteBuffer[] input, ByteBuffer initializationVector) {
    Cipher cipher = getCipher(currentSecretKey, Cipher.ENCRYPT_MODE, initializationVector);

    List<ByteBuffer> outputs = new ArrayList<>();

    for (ByteBuffer inputBuffer : input) {
      int size = cipher.getOutputSize(inputBuffer.remaining());
      int retries = 3;
      while (retries > 0) {
        try {
          ByteBuffer cipherBuffer = (inputBuffer.isDirect() ? ByteBuffer.allocateDirect(size)
              : ByteBuffer.allocate(size));
          cipher.update(inputBuffer, cipherBuffer);
          outputs.add((ByteBuffer) cipherBuffer.flip());
          break;
        } catch (ShortBufferException e) {
          size = size + (size * 10 / 100);
          --retries;
        }
      }
      if (retries == 0) {
        throw new IllegalArgumentException("fail to cipher data");
      }
    }

    try {
      byte[] tail = cipher.doFinal();
      if (tail.length > 0) {
        outputs.add(ByteBuffer.wrap(tail));
      }

      return outputs.toArray(new ByteBuffer[0]);
    } catch (IllegalBlockSizeException e) {
      throw new IllegalArgumentException("invalid block size to cipher the data", e);
    } catch (BadPaddingException e) {
      throw new IllegalArgumentException("fail to cipher data as it is not padded properly", e);
    }
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer cipherBuffer, ByteBuffer ivBuffer, String token) {
    SecretKey secretKey = tokenToKey.get(token);
    if(secretKey == null) {
      throw new AssertionError(String.format("Invalid token: %s", token));
    }
    Cipher cipher = getCipher(secretKey, Cipher.DECRYPT_MODE, ivBuffer);
    int retries = 3;
    int size = cipher.getOutputSize(cipherBuffer.capacity());
    while (retries > 0) {
      try {
        ByteBuffer plainBuffer = (cipherBuffer.isDirect() ? ByteBuffer.allocateDirect(size)
            : ByteBuffer.allocate(size));
        cipher.doFinal(cipherBuffer, plainBuffer);
        plainBuffer.flip();
        return plainBuffer;
      } catch (IllegalBlockSizeException e) {
        throw new IllegalArgumentException("invalid block size to decipher the data", e);
      } catch (BadPaddingException e) {
        throw new IllegalArgumentException("fail to decipher data as it is not padded properly", e);
      } catch (ShortBufferException e) {
        size = size + (size * 10 / 100);
        --retries;
      }
    }
    throw new IllegalArgumentException("fail to decipher data");
  }

  @Override
  public String getCurrentToken() {
    return currentToken;
  }

  @Override
  public Optional<String> getPreviousToken() {
    return tokenToKey.keySet().stream().filter(k -> !k.equals(getCurrentToken())).findFirst();
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
  public void remove(String token) {
    tokenToKey.remove(token);
  }
}
