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

import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

public class LazyDecryptingGettableAction implements GettableAction, EncryptedAction {

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final CipherManager cipherManager;
  private final ByteBuffer identifier;
  private final long invalidatedLsn;
  private final String token;
  private final ByteBuffer[] buffers;
  private final ActionCodec codec;
  
  private Closeable disposable;
  private volatile GettableAction action;

  public LazyDecryptingGettableAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                      CipherManager cipherManager, long invalidatedLsn, ByteBuffer identifier,
                                      String token, ByteBuffer[] buffers, ActionCodec codec) {
    this.objectManager = objectManager;
    this.cipherManager = cipherManager;
    this.invalidatedLsn = invalidatedLsn;
    this.token = token;
    this.identifier = identifier;
    this.buffers = buffers;
    this.codec = codec;
  }

  @Override
  public long getLsn() {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void setDisposable(Closeable c) {
    disposable = c;
  }

  @Override
  public void dispose() {
    try {
      this.close();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ByteBuffer getIdentifier() {
    return identifier;
  }

  @Override
  public ByteBuffer getKey() {
    if (action == null) {
      action = decrypt();
    }
    return action.getKey();
  }

  @Override
  public ByteBuffer getValue() {
    if (action == null) {
      action = decrypt();
    }
    return action.getValue();
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void record(long lsn) {

  }

  @Override
  public void replay(long lsn) {
    action = decrypt();
    objectManager.replayPut(getIdentifier(), getKey(), getValue(), lsn);
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    throw new UnsupportedOperationException("action cannot be serialized");
  }

  @Override
  public void close() throws IOException {
    if (disposable != null) {
      disposable.close();
      disposable = null;
    }
  }

  @Override
  public String getToken() {
    return token;
  }
  
  private GettableAction decrypt() {
    int ivLength = ByteBufferUtils.getInt(buffers);
    int payloadLength = ByteBufferUtils.getInt(buffers);
    ByteBuffer initializationVector = ByteBufferUtils.getBytes(ivLength, buffers);
    ByteBuffer encryptedPayload = ByteBufferUtils.getBytes(payloadLength, buffers);

    ByteBuffer payload = cipherManager.decrypt(encryptedPayload, initializationVector, token);
    return (GettableAction) codec.decode(new ByteBuffer[]{payload});
  }
}
