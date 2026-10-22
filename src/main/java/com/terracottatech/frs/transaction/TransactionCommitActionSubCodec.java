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
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;

public class TransactionCommitActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, TransactionCommitAction> {
  @Override
  public ByteBuffer[] encode(TransactionCommitAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    TransactionHandle handle = action.getHandle();
    ByteBuffer handleBuffer = handle.toByteBuffer();
    ByteBuffer header = ByteBuffer.allocate(1);
    if (action.isBegin()) {
      header.put((byte) 1);
    } else {
      header.put((byte) 0);
    }
    header.flip();
    return new ByteBuffer[]{handleBuffer, header};
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, 
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    TransactionHandle handle = TransactionHandleImpl.withByteBuffers(buffers);
    boolean emptyTransaction = ByteBufferUtils.get(buffers) == 1;
    return new TransactionCommitAction(handle, emptyTransaction);
  }
}
