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
package com.terracottatech.frs;

import java.nio.ByteBuffer;
import java.security.AlgorithmParameters;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidParameterSpecException;
import java.util.List;

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
  private final List<SecretKey> secretKeys;

  public AESCipherManager(Configuration config, List<byte []> keys) {
    algorithm = config.getString(FrsProperty.STORE_CIPHER_ALGORITHM);
    secretKeys = keys.stream()
      .map(key -> (SecretKey) new SecretKeySpec(key, "AES"))
      .collect(java.util.stream.Collectors.toList());
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
    byte [] iv;
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
    byte [] iv;
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
  public ByteBuffer encrypt(ByteBuffer plainBuffer, ByteBuffer ivBuffer) {
    SecretKey secretKey = secretKeys.get(0);
    Cipher cipher = getCipher(secretKey, Cipher.ENCRYPT_MODE, ivBuffer);
    int retries = 3;
    int size = cipher.getOutputSize(plainBuffer.capacity());
    while (retries > 0) {
      try {
        ByteBuffer cipherBuffer =
          (plainBuffer.isDirect() ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size));
        cipher.doFinal(plainBuffer, cipherBuffer);
        cipherBuffer.flip();
        return cipherBuffer;
      } catch (IllegalBlockSizeException e) {
        throw new IllegalArgumentException("invalid block size to cipher the data", e);
      } catch (BadPaddingException e) {
        throw new IllegalArgumentException("fail to cipher data as it is not padded properly", e);
      } catch (ShortBufferException e) {
        size = size + (size * 10 / 100);
        --retries;
      }
    }
    throw new IllegalArgumentException("fail to cipher data");
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer cipherBuffer, ByteBuffer ivBuffer) {
    SecretKey secretKey = secretKeys.get(0);
    Cipher cipher = getCipher(secretKey, Cipher.DECRYPT_MODE, ivBuffer);
    int retries = 3;
    int size = cipher.getOutputSize(cipherBuffer.capacity());
    while (retries > 0) {
      try {
        ByteBuffer plainBuffer =
          (cipherBuffer.isDirect() ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size));
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
}
