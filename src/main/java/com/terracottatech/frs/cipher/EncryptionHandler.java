package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;

import java.util.Optional;

public interface EncryptionHandler {

  Optional<String> getPreviousToken();

  boolean isUsingEncKey(String token);

  void add(String token, byte[] key);

  void remove(String token);

  Action convert(Action action);
}
