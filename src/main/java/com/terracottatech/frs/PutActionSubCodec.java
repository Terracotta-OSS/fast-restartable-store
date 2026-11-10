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
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionSubCodec;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.PutAction.HEADER_SIZE;

public class PutActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, PutAction> {
  /* PutAction.getPayload
  4 bytes - PutAction.idByteCount
  4 bytes - PutAction.keyByteCount
  4 bytes - PutAction.valueByteCount
  8 bytes - PutAction.invalidatedLsn
  */
  @Override
  public ByteBuffer[] encode(PutAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    ByteBuffer id = action.getIdentifier();
    ByteBuffer key = action.getKey();
    ByteBuffer value = action.getValue();
    
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    header.putInt(id.remaining());
    header.putInt(key.remaining());
    header.putInt(value.remaining());

    long invalidatedLsn = action.getInvalidatedLsns().stream().reduce(
        (a, b) -> {
          throw new IllegalStateException("More than one element is present");
        }).orElse(-1L);
    header.putLong(invalidatedLsn).flip();
    return new ByteBuffer[]{header, id.slice(), key.slice(), value.slice()};
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, 
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    int idLength = ByteBufferUtils.getInt(buffers);
    int keyLength = ByteBufferUtils.getInt(buffers);
    int valueLength = ByteBufferUtils.getInt(buffers);
    long invalidatedLsn = ByteBufferUtils.getLong(buffers);
    ByteBuffer id = ByteBufferUtils.getBytes(idLength, buffers);
    ByteBuffer key = ByteBufferUtils.getBytes(keyLength, buffers);
    ByteBuffer value = ByteBufferUtils.getBytes(valueLength, buffers);
    return new PutAction(objectManager, null, id, key, value, invalidatedLsn);
  }
}
