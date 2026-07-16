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

public class DeleteActionSubCodec implements ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, DeleteAction> {
  @Override
  public ByteBuffer[] encode(DeleteAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
    return new ByteBuffer[] { action.getId().slice() };
  }

  @Override
  public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, 
                       ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
    return new DeleteAction(objectManager, null, ByteBufferUtils.getFirstNonEmpty(buffers), false);
  }
}
