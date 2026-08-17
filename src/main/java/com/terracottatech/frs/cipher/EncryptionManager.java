package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;

import java.util.Optional;

public interface EncryptionManager {
  /**
   * Gets the token identifying the previously used encryption key, if any.
   *
   * @return an Optional containing the previous key token, or empty if no previous key exists
   */
  Optional<String> getPreviousToken();

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
   * Removes the encryption key identified by the given token.
   *
   * @param token the token identifying the encryption key to remove
   */
  void remove(String token);
  
  Action convert(Action action);
}
