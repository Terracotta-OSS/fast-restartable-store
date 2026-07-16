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
package com.terracottatech.frs.action;

import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class NullAction implements Action {
  public static final Action INSTANCE = new NullAction();
  
  private long lsn;

  public static ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, NullAction> subCodec() {
    return new ActionSubCodec<ByteBuffer, ByteBuffer, ByteBuffer, NullAction>() {

      @Override
      public ByteBuffer[] encode(NullAction action, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec) {
        return new ByteBuffer[0];
      }

      @Override
      public Action decode(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, ActionCodec<ByteBuffer, ByteBuffer, ByteBuffer> codec, ByteBuffer[] buffers) {
        return INSTANCE;
      }
    };
  }

  @Override
  public void record(long lsn) {
      this.lsn = lsn;
  }
  
  public long getLsn() {
      return lsn;
  }

  @Override
  public void replay(long lsn) {
  }
}
