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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionSubCodec;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.transaction.TransactionalAction.BEGIN_BIT;
import static com.terracottatech.frs.transaction.TransactionalAction.COMMIT_BIT;
import static com.terracottatech.frs.util.ByteBufferUtils.concatenate;
import static com.terracottatech.frs.util.ByteBufferUtils.get;

public class TransactionalActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, TransactionalAction> {
  @Override
  public ByteBuffer[] encode(TransactionalAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    
    TransactionHandle handle = action.getHandle();
    ByteBuffer handleBuffer = handle.toByteBuffer();
    ByteBuffer header = ByteBuffer.allocate(handleBuffer.capacity() + 1);

    byte tempMode = 0;
    if (action.isCommit()) {
      tempMode |= COMMIT_BIT;
    }
    if (action.isBegin()) {
      tempMode |= BEGIN_BIT;
    }
    header.put(handleBuffer).put(tempMode).flip();
    return concatenate(header, codec.encode(action.getAction()));
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, 
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    return new TransactionalAction(
        TransactionHandleImpl.withByteBuffers(buffers), get(buffers), codec.decode(buffers));
  }
}
