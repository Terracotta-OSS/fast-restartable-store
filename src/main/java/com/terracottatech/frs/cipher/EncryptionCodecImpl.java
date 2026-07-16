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

import com.terracottatech.frs.DeleteAction;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.RemoveAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionSubCodec;
import com.terracottatech.frs.action.NullAction;
import com.terracottatech.frs.compaction.CompactionAction;
import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.config.FrsProperty;
import com.terracottatech.frs.transaction.TransactionCommitAction;
import com.terracottatech.frs.transaction.TransactionalAction;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.Collection;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EncryptionCodecImpl implements EncryptionCodec<ByteBuffer, ByteBuffer, ByteBuffer> {

  public static final String TOKEN_KEY_DELIMITER = ":";
  public static final String MULTIPLE_TOKEN_KEY_DELIMITER = ",";

  private static final Map<Class<? extends Action>, TriConsumer> ENC_BINDINGS = new HashMap<>();

  static {
    ENC_BINDINGS.put(NullAction.class, transparent());
    ENC_BINDINGS.put(RemoveAction.class, transparent());
    ENC_BINDINGS.put(DeleteAction.class, transparent());
    ENC_BINDINGS.put(TransactionalAction.class, transparent());
    ENC_BINDINGS.put(TransactionCommitAction.class, transparent());
    ENC_BINDINGS.put(PutAction.class, (codec, collectionId, actionId) ->
        codec.bind(PutAction.class, convertCodecId(collectionId), actionId));
    ENC_BINDINGS.put(CompactionAction.class, (codec, collectionId, actionId) ->
        codec.bind(CompactionAction.class, convertCodecId(collectionId), actionId));
  }

  private static int convertCodecId(int collectionId) {
    assert (collectionId < 99);
    return 99 + collectionId;
  }

  private static TriConsumer transparent() {
    return (codec, collectionId, actionId) -> {
    };
  }

  private final ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> actionCodec;
  private final CipherManager cipherManager;
  private final Map<Class<? extends Action>, int[]> plainActionId = new ConcurrentHashMap<>();
  private volatile boolean encryptEnabled = false;

  public EncryptionCodecImpl(Configuration configuration,
                             ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> actionCodec) {
    this.actionCodec = actionCodec;
    boolean encrypted = configuration.getBoolean(FrsProperty.STORE_ENCRYPTION_ENABLE);
    if (encrypted) {
      String oldTokenAndKeys = configuration.getString(FrsProperty.STORE_ENCRYPTION_OLD_TOKENS_AND_KEYS);
      String newTokenAndKey = configuration.getString(FrsProperty.STORE_ENCRYPTION_NEW_TOKEN_AND_KEY);
      Map<String, byte[]> tokenToKeyMap = new HashMap<>();

      if (oldTokenAndKeys != null) {
        String[] oldTokensSplit = oldTokenAndKeys.split(MULTIPLE_TOKEN_KEY_DELIMITER);
        for (String s : oldTokensSplit) {
          String[] oldTokenAndKey = s.split(TOKEN_KEY_DELIMITER);
          String oldToken = oldTokenAndKey[0];
          byte[] oldKey = Base64.getDecoder().decode(oldTokenAndKey[1]);
          tokenToKeyMap.put(oldToken, oldKey);
        }
      }

      if (newTokenAndKey == null) {
        throw new IllegalArgumentException("token and key must be provided when running with encryption enabled");
      }

      String[] newTokenSplit = newTokenAndKey.split(TOKEN_KEY_DELIMITER);
      String newToken = newTokenSplit[0];
      byte[] newKey = Base64.getDecoder().decode(newTokenSplit[1]);
      tokenToKeyMap.put(newToken, newKey);

      cipherManager = new AESCipherManager(tokenToKeyMap, newToken);
      encryptEnabled = true;
    } else {
      cipherManager = new AESCipherManager();
    }
    registerCodec(convertCodecId(0), 0, new EncryptedPutActionSubCodec(cipherManager));
    registerCodec(convertCodecId(2), 0, new EncryptedPutActionSubCodec(cipherManager)); // needed beacuse of Compaction action collection id
  }

  @Override
  public String getCurrToken() {
    return cipherManager.getCurrentToken();
  }

  @Override
  public Collection<String> getPreviousTokens() {
    return cipherManager.getPreviousTokens();
  }

  @Override
  public boolean isUsingEncKey(String token) {
    return cipherManager.isUsingEncKey(token);
  }

  @Override
  public void add(String token, byte[] key) {
    cipherManager.add(token, key);
    if (!encryptEnabled) {
      encryptEnabled = true;
      updateBindings();
    }
  }

  private void updateBindings() {
    plainActionId.forEach((cl, res) -> {
      TriConsumer consumer = ENC_BINDINGS.get(cl);
      consumer.accept(actionCodec, res[0], res[1]);
    });
  }

  @Override
  public void remove(Collection<String> tokens) {
    cipherManager.remove(tokens);
  }

  @Override
  public <T extends Action> void registerCodec(int collectionId, int actionId,
                                               ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, ? super T> actionSubCodec) {
    actionCodec.registerCodec(collectionId, actionId, actionSubCodec);
  }

  @Override
  public void bind(Class<? extends Action> actionClass, int collectionId, int actionId) {
    if (!ENC_BINDINGS.containsKey(actionClass)) {
      throw new IllegalArgumentException("No encryption action register for: " + actionClass);
    }
    actionCodec.bind(actionClass, collectionId, actionId);
    if (encryptEnabled) {
      ENC_BINDINGS.get(actionClass).accept(actionCodec, collectionId, actionId);
    } else {
      // store collectionId and actionId
      plainActionId.put(actionClass, new int[]{collectionId, actionId});
    }
  }

  @Override
  public Action decode(ByteBuffer[] buffer) {
    return actionCodec.decode(buffer);
  }

  @Override
  public ByteBuffer[] encode(Action action) {
    return actionCodec.encode(action);
  }

  @FunctionalInterface
  private interface TriConsumer {
    void accept(ActionCodec<?, ?, ?> codec, int collectionId, int actionId);
  }
}
