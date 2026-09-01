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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.GCMParameterSpec;

/**
 * Default {@link CipherAlgorithmDelegate} using AES/GCM/NoPadding.
 */
public class AESGCMCipherDelegate implements CipherAlgorithmDelegate {

  static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final int GCM_TAG_LENGTH_BITS = 128;
  private static final int GCM_IV_LENGTH_BYTES = 12;

  @Override
  public Cipher getCipher() {
    try {
      return Cipher.getInstance(ALGORITHM);
    } catch (NoSuchAlgorithmException ex) {
      throw new IllegalArgumentException("invalid cipher algorithm", ex);
    } catch (NoSuchPaddingException ex) {
      throw new IllegalArgumentException("padding mechanism not available", ex);
    }
  }

  @Override
  public Cipher getCipher(SecretKey secretKey, int operation, ByteBuffer ivBuffer) {
    Cipher cipher = getCipher();
    ByteBuffer ivDuplicate = ivBuffer.duplicate();
    byte[] iv = new byte[ivDuplicate.remaining()];
    ivDuplicate.get(iv);
    try {
      cipher.init(operation, secretKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
      return cipher;
    } catch (InvalidKeyException e) {
      throw new IllegalArgumentException("invalid cipher key", e);
    } catch (InvalidAlgorithmParameterException e) {
      throw new IllegalArgumentException("invalid parameter for cipher algorithm", e);
    }
  }

  @Override
  public ByteBuffer generateInitializationVector() {
    byte[] iv = new byte[GCM_IV_LENGTH_BYTES];
    new SecureRandom().nextBytes(iv);
    return ByteBuffer.wrap(iv);
  }

  @Override
  public ByteBuffer[] encrypt(ByteBuffer[] input, SecretKey secretKey, ByteBuffer initializationVector) {
    Cipher cipher = getCipher(secretKey, Cipher.ENCRYPT_MODE, initializationVector);

    int totalInputSize = 0;
    for (ByteBuffer buf : input) {
      totalInputSize += buf.remaining();
    }

    int totalOutputSize = cipher.getOutputSize(totalInputSize); // includes GCM tag
    boolean useDirect = input.length == 0 ? false : input[0].isDirect();
    ByteBuffer cipherBuffer = useDirect ? ByteBuffer.allocateDirect(totalOutputSize) :
        ByteBuffer.allocate(totalOutputSize);

    for (ByteBuffer inputBuffer : input) {
      try {
        cipher.update(inputBuffer, cipherBuffer);
      } catch (ShortBufferException e) {
        throw new IllegalStateException("unexpected ShortBufferException during GCM encrypt update", e);
      }
    }
    ByteBuffer emptyInput = ByteBuffer.allocate(0);
    try {
      cipher.doFinal(emptyInput, cipherBuffer);
    } catch (ShortBufferException e) {
      throw new IllegalStateException("unexpected ShortBufferException during GCM encrypt update", e);
    } catch (IllegalBlockSizeException e) {
      throw new IllegalArgumentException("invalid block size to cipher the data", e);
    } catch (BadPaddingException e) {
      throw new IllegalArgumentException("fail to cipher data as it is not padded properly", e);
    }

    cipherBuffer.flip();
    return new ByteBuffer[]{cipherBuffer};
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer cipherBuffer, SecretKey secretKey, ByteBuffer ivBuffer) {
    Cipher cipher = getCipher(secretKey, Cipher.DECRYPT_MODE, ivBuffer);
    int size = cipher.getOutputSize(cipherBuffer.remaining());
    ByteBuffer plainBuffer = cipherBuffer.isDirect()
        ? ByteBuffer.allocateDirect(size)
        : ByteBuffer.allocate(size);
    try {
      cipher.doFinal(cipherBuffer, plainBuffer);
      plainBuffer.flip();
      return plainBuffer;
    } catch (IllegalBlockSizeException e) {
      throw new IllegalArgumentException("invalid block size to decipher the data", e);
    } catch (BadPaddingException e) {
      throw new IllegalArgumentException("fail to decipher data as it is not padded properly", e);
    } catch (ShortBufferException e) {
      // getOutputSize guarantees the buffer is large enough for GCM — this cannot happen
      throw new IllegalStateException("unexpected ShortBufferException during GCM decrypt", e);
    }
  }
}
