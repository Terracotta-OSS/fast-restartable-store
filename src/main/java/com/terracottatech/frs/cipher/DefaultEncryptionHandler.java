package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.config.Configuration;

import java.util.Map;
import java.util.Optional;

public class DefaultEncryptionHandler implements EncryptionHandler {
  
  private final CipherManager cipherManager;
  
  public DefaultEncryptionHandler(Configuration config, ActionCodec codec, Map<String, byte[]> tokenToKeyMap, String currentToken) {
    cipherManager = new AESCipherManager(config, tokenToKeyMap, currentToken);
    EncryptionActions.registerActions(3, codec, cipherManager);
  }
  
  @Override
  public Optional<String> getPreviousToken() {
    return cipherManager.getPreviousToken();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return cipherManager.isUsingEncKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    cipherManager.add(token, key);
  }

  @Override
  public void remove(String token) {
    cipherManager.remove(token);
  }

  @Override
  public Action convert(Action action) {
    return new EncryptedAction(action, cipherManager);
  }
}
