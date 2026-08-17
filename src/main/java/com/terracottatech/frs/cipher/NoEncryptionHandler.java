package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;

import java.util.Optional;

public class NoEncryptionHandler implements EncryptionHandler {

  @Override
  public Optional<String> getPreviousToken() {
    return Optional.empty();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return false;
  }

  @Override
  public void add(String token, byte[] key) {
    throw new UnsupportedOperationException("operation unsupported");
  }

  @Override
  public void remove(String token) {
    throw new UnsupportedOperationException("operation unsupported");
  }

  @Override
  public Action convert(Action action) {
    return action;
  }
}
