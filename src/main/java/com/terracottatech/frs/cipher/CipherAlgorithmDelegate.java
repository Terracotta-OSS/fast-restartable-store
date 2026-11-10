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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

/**
 * Delegate interface that encapsulates the algorithm-specific cipher operations
 * used by {@link AESCipherManager}.Implementations are responsible for knowing 
 * how to create and initialise {@link Cipher} instances, generate initialization 
 * vectors, encrypt plaintext and decrypt ciphertext for a particular algorithm (e.g. AES/GCM/NoPadding).
 */
public interface CipherAlgorithmDelegate {

  /**
   * Returns a fresh, uninitialised {@link Cipher} instance for the algorithm
   * handled by this delegate.
   */
  Cipher getCipher();

  /**
   * Returns a {@link Cipher} initialised for the given {@code operation}
   * ({@link Cipher#ENCRYPT_MODE} or {@link Cipher#DECRYPT_MODE}) using the
   * supplied key and initialization vector.
   *
   * @param secretKey  the secret key to use
   * @param operation  {@code Cipher.ENCRYPT_MODE} or {@code Cipher.DECRYPT_MODE}
   * @param ivBuffer   the initialization vector
   */
  Cipher getCipher(SecretKey secretKey, int operation, ByteBuffer ivBuffer);

  /**
   * Generates a secure initialization vector suitable for the algorithm
   * handled by this delegate.
   *
   * @return a {@link ByteBuffer} containing the IV bytes
   */
  ByteBuffer generateInitializationVector();

  /**
   * Encrypts each buffer in {@code input} using the supplied key and IV,
   * returning the resulting ciphertext buffers.
   *
   * @param input                the plaintext buffers
   * @param secretKey            the key to encrypt with
   * @param initializationVector the IV to use
   * @return encrypted {@link ByteBuffer} array
   */
  ByteBuffer[] encrypt(ByteBuffer[] input, SecretKey secretKey, ByteBuffer initializationVector);

  /**
   * Decrypts {@code cipherBuffer} using the supplied key and IV.
   *
   * @param cipherBuffer the ciphertext
   * @param secretKey    the key to decrypt with
   * @param ivBuffer     the IV that was used during encryption
   * @return decrypted plaintext as a {@link ByteBuffer}
   */
  ByteBuffer decrypt(ByteBuffer cipherBuffer, SecretKey secretKey, ByteBuffer ivBuffer);
}
