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

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionSubCodec;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EncryptedPutActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, PutAction> {
  private final CipherManager cipherManager;

  public EncryptedPutActionSubCodec(CipherManager cipherManager) {
    this.cipherManager = cipherManager;
  }

  @Override
  public ByteBuffer[] encode(PutAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    ByteBuffer initializationVector = cipherManager.generateInitializationVector();

    ByteBuffer identifier = action.getIdentifier().duplicate();
    ByteBuffer key = action.getKey().duplicate();
    ByteBuffer value = action.getValue().duplicate();

    long invalidatedLsn = action.getInvalidatedLsns().stream().reduce(
        (a, b) -> {
          throw new IllegalStateException("More than one element is present");
        }).orElse(-1L);
    byte[] ctoken = cipherManager.getCurrentToken().getBytes(StandardCharsets.UTF_8);
    // 16 for lsn, identifier length, ctoken length
    int size = identifier.remaining() + ctoken.length + 16;
    ByteBuffer metaData = ByteBuffer.allocate(size);
    metaData.putLong(invalidatedLsn);
    metaData.putInt(identifier.remaining());
    metaData.put(identifier);
    metaData.putInt(ctoken.length);
    metaData.put(ctoken);

    ByteBuffer payloadHeader = ByteBuffer.allocate(8);
    payloadHeader.putInt(key.remaining());
    payloadHeader.putInt(value.remaining());
    payloadHeader.flip();
    ByteBuffer[] payload = new ByteBuffer[]{payloadHeader, key, value};
    ByteBuffer[] encryptedValue = cipherManager.encrypt(payload, initializationVector);
    int length = 0;
    for (ByteBuffer buffer : encryptedValue) {
      length += buffer.remaining();
    }

    ByteBuffer[] output = new ByteBuffer[encryptedValue.length + 3];

    output[0] = metaData;
    output[1] = ByteBuffer.allocate(8)
        .putInt(initializationVector.remaining())
        .putInt(length);

    output[0].flip();
    output[1].flip();
    output[2] = initializationVector.asReadOnlyBuffer();
    System.arraycopy(encryptedValue, 0, output, 3, encryptedValue.length);
    return output;
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    long invalidatedLsn = ByteBufferUtils.getLong(buffers);
    int len = ByteBufferUtils.getInt(buffers);
    ByteBuffer identifier = ByteBufferUtils.getBytes(len, buffers);
    int tokenLength = ByteBufferUtils.getInt(buffers);
    ByteBuffer tokenBuffer = ByteBufferUtils.getBytes(tokenLength, buffers);
    String token = StandardCharsets.UTF_8.decode(tokenBuffer).toString();
    return new LazyDecryptingGettableAction(objectManager, cipherManager, invalidatedLsn, identifier, token, buffers);
  }
}
