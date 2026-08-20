package com.terracottatech.frs.cipher;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EncryptionManagerImpl implements EncryptionManager {

  private final ActionCodec actionCodec;
  private final Configuration configuration;

  private volatile EncryptionHandler cipherKeyHandler;
  private volatile boolean encryptEnabled = false;

  public EncryptionManagerImpl(Configuration configuration, ActionCodec actionCodec) {
    this.configuration = configuration;
    this.actionCodec = actionCodec;
    boolean encrypted = configuration.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE);
    if (encrypted) {
      String oldToken = configuration.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKEN);
      byte[] oldKey = configuration.getByteArray(FrsProperty.STORE_ENCRYPTION_OLD_KEY);
      String newToken = configuration.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN);
      byte[] newKey = configuration.getByteArray(FrsProperty.STORE_ENCRYPTION_NEW_KEY);

      Map<String, byte[]> tokenToKeyMap = new HashMap<>();
      if (oldToken != null && oldKey != null) {
        tokenToKeyMap.put(oldToken, oldKey);
      }
      tokenToKeyMap.put(newToken, newKey);
      cipherKeyHandler = new DefaultEncryptionHandler(configuration, actionCodec, tokenToKeyMap, newToken);
      encryptEnabled = true;
    } else {
      cipherKeyHandler = new NoEncryptionHandler();
    }
  }

  @Override
  public Optional<String> getPreviousToken() {
    return cipherKeyHandler.getPreviousToken();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return cipherKeyHandler.isUsingEncKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    if (encryptEnabled) {
      cipherKeyHandler.add(token, key);
    } else {
      Map<String, byte[]> tokenToKeyMap = new HashMap<>();
      tokenToKeyMap.put(token, key);
      cipherKeyHandler = new DefaultEncryptionHandler(configuration, actionCodec, tokenToKeyMap, token);
    }
  }

  @Override
  public void remove(String token) {
    cipherKeyHandler.remove(token);
  }

  @Override
  public Action convert(Action action) {
    return cipherKeyHandler.convert(action);
  }
}
