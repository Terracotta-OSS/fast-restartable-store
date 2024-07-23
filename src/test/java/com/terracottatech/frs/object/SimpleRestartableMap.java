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
package com.terracottatech.frs.object;

import com.terracottatech.frs.RestartStore;

import java.nio.ByteBuffer;

import static com.terracottatech.frs.util.TestUtils.byteBufferWithInt;

/**
 * @author tim
 */
public class SimpleRestartableMap extends RestartableMap<String, String, ByteBuffer, ByteBuffer, ByteBuffer> {

  public SimpleRestartableMap(int identifier, RestartStore<ByteBuffer, ByteBuffer, ByteBuffer> restartability, boolean synchronousWrites) {
    super(byteBufferWithInt(identifier), restartability, synchronousWrites);
  }

  @Override
  protected ByteBuffer encodeKey(String key) {
    return ByteBuffer.wrap(key.getBytes());
  }

  @Override
  protected ByteBuffer encodeValue(String value) {
    return ByteBuffer.wrap(value.getBytes());
  }

  @Override
  protected String decodeKey(ByteBuffer rKey) {
    byte[] buf = new byte[rKey.remaining()];
    rKey.slice().get(buf);
    return new String(buf);
  }

  @Override
  protected String decodeValue(ByteBuffer rValue) {
    byte[] buf = new byte[rValue.remaining()];
    rValue.slice().get(buf);
    return new String(buf);
  }

  @Override
  protected long keyByteSize(String key, ByteBuffer encodedKey) {
    return encodedKey.remaining();
  }

  @Override
  protected long valueByteSize(String value, ByteBuffer encodedValue) {
    return encodedValue.remaining();
  }
}
