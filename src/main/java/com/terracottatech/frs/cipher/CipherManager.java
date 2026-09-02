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
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for managing encryption and decryption operations in the Fast Restartable Store.
 *
 * <p>
 * CipherManager provides methods to encrypt and decrypt data stored in ByteBuffers, allowing
 * secure storage of sensitive information. Implementations of this interface handle the
 * cryptographic operations while hiding the complexity of the underlying encryption algorithms and
 * key management.
 */
public interface CipherManager {

  /**
   * Generates a secure initialization vector (IV) for encryption operations.
   *
   * <p>
   * An initialization vector is a random or pseudo-random value that is used along with
   * the encryption key to ensure that encrypting the same plaintext multiple times produces
   * different ciphertext outputs. This helps prevent pattern analysis and increases security.
   *
   * <p>
   * The generated IV should be stored alongside the encrypted data as it will be required
   * for decryption.
   *
   * @return A ByteBuffer containing the generated initialization vector
   */
  ByteBuffer generateInitializationVector();

  /**
   * Encrypts the content of a ByteBuffer using the provided initialization vector.
   *
   * @param plainBuffer The ByteBuffer array containing the plain text data to be encrypted
   * @param ivBuffer The initialization vector to use for the encryption operation
   * @return An array of ByteBuffer containing the encrypted data
   */
  ByteBuffer[] encrypt(ByteBuffer[] plainBuffer, ByteBuffer ivBuffer);

  /**
   * Decrypts the content of a ByteBuffer using the provided initialization vector.
   *
   * @param cipherBuffer The ByteBuffer containing the encrypted data to be decrypted
   * @param ivBuffer The initialization vector to use for the decryption operation
   * @return A ByteBuffer containing the decrypted plain text data
   */
  ByteBuffer decrypt(ByteBuffer cipherBuffer, ByteBuffer ivBuffer, String token);

  
  /**
   * Gets the token identifying the encryption key currently in use.
   *
   * @return the token of the current encryption key
   */
  String getCurrentToken();

  /**
   * Gets the token identifying the previously used encryption key, if any.
   *
   * @return list containing the previous tokens
   */
  List<String> getPreviousTokens();

  /**
   * Checks if the cipher manager is currently using the encryption key identified by the given token.
   *
   * @param token the token identifying the encryption key to check
   * @return true if the specified encryption key is currently in use, false otherwise
   */
  boolean isUsingEncKey(String token);

  /**
   * Adds a new encryption key to the cipher manager with the specified token identifier.
   *
   * @param token the token to identify this encryption key
   * @param key the encryption key bytes to add
   */
  void add(String token, byte[] key);

  /**
   * Removes the encryption key identified by the given token from the cipher manager.
   *
   * @param tokens list of tokens identifying the encryption key to remove
   */
  void remove(List<String> tokens);
}
