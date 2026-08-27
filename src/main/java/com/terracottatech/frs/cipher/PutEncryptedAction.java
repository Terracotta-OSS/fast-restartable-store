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
import java.nio.charset.StandardCharsets;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

public class PutEncryptedAction implements Action {

  public static class EncryptedActionFactory implements ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> {

    private final CipherManager cipherManager;

    public EncryptedActionFactory(CipherManager cipherManager) {
      this.cipherManager = cipherManager;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                         ActionCodec codec, ByteBuffer[] buffers) {
      
      long invalidatedLsn = ByteBufferUtils.getLong(buffers);
      int len = ByteBufferUtils.getInt(buffers);
      ByteBuffer identifier = ByteBufferUtils.getBytes(len, buffers);

      return new LazyDecryptingPutAction(objectManager, cipherManager, invalidatedLsn, identifier, buffers, codec);
    }
  }

  private static final int HEADER_SIZE = ByteBufferUtils.INT_SIZE * 3;

  private final PutAction delegate;
  private final CipherManager cipherManager;
  // buffer contains initialization vector which introduces randomness and
  // ensure same plaintext encrypts to different ciphertexts each time
  private final ByteBuffer initializationVector;

  public PutEncryptedAction(PutAction delegate, CipherManager cipherManager) {
    this.delegate = delegate;
    this.cipherManager = cipherManager;
    this.initializationVector = cipherManager.generateInitializationVector();
  }

  @Override
  public void record(long lsn) {
    delegate.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    delegate.replay(lsn);
  }

  @Override
  public int replayConcurrency() {
    return delegate.replayConcurrency();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    // This is where we modify the payload to include the encrypted value
    // and initialization vector instead of the original value
    ByteBuffer[] delegatePayload = codec.encode(delegate);

    ByteBuffer identifier = delegate.getIdentifier();
    long invalidatedLsn = delegate.getInvalidatedLsns().stream().findFirst().get();
    ByteBuffer metaData = ByteBuffer.allocate(identifier.remaining() + 12);
    metaData.putLong(invalidatedLsn);
    metaData.putInt(identifier.remaining());
    metaData.put(identifier.duplicate());

    ByteBuffer[] encryptedValue = cipherManager.encrypt(delegatePayload, initializationVector);
    int length = 0;
    for (ByteBuffer buffer : encryptedValue) {
      length += buffer.remaining();
    }

    ByteBuffer[] output = new ByteBuffer[encryptedValue.length + 4];

    output[0] = metaData;
    byte[] ctoken = cipherManager.getCurrentToken().getBytes(StandardCharsets.UTF_8);
    output[1] = ByteBuffer.allocate(HEADER_SIZE)
        .putInt(initializationVector.remaining()).putInt(ctoken.length)
        .putInt(length);
    
    output[0].flip();
    output[1].flip();
    output[2] = initializationVector.asReadOnlyBuffer();
    output[3] = ByteBuffer.wrap(ctoken);
    System.arraycopy(encryptedValue, 0, output, 4, encryptedValue.length);
    return output;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PutEncryptedAction that = (PutEncryptedAction) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  public Action getDelegate() {
    return delegate;
  }
}
