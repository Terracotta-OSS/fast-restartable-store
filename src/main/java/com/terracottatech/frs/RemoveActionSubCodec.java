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
import com.terracottatech.frs.action.SimpleInvalidatingAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Collections;

public class RemoveActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, RemoveAction> {
  @Override
  public ByteBuffer[] encode(RemoveAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    long invalidatedLsn = action.getInvalidatedLsns().stream().reduce(
        (a, b) -> {
          throw new IllegalStateException("More than one element is present");
        }).orElse(-1L);
    ByteBuffer header = ByteBuffer.allocate(ByteBufferUtils.LONG_SIZE);
    header.putLong(invalidatedLsn).flip();
    return new ByteBuffer[] { header };
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, 
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    long invalidatedLsn = ByteBufferUtils.getLong(buffers);
    return new SimpleInvalidatingAction(Collections.singleton(invalidatedLsn));
  }
}
