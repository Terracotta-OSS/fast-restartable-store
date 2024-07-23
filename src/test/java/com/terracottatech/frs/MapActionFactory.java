/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;

/**
 * @author tim
 */
public class MapActionFactory {
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final Compactor compactor;

  public MapActionFactory(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                          Compactor compactor) {
    this.objectManager = objectManager;
    this.compactor = compactor;
  }

  public Action put(int i, int k, int v) {
    return new PutAction(objectManager, compactor, byteBufferWithInt(i),
                         byteBufferWithInt(k), byteBufferWithInt(v), false);
  }

  public Action put(int i, int k, int v, long lsn) {
    return new PutAction(objectManager, compactor, byteBufferWithInt(i),
                         byteBufferWithInt(k), byteBufferWithInt(v), lsn);
  }

  public Action remove(int i, int k) {
    return new RemoveAction(objectManager, compactor, byteBufferWithInt(i),
                            byteBufferWithInt(k), false);
  }

  public Action delete(int i) {
    return new DeleteAction(objectManager, compactor, byteBufferWithInt(i), false);
  }
}
