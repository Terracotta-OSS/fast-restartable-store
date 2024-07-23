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
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.util.ByteBufferUtils;

import java.nio.ByteBuffer;

/**
* @author tim
*/
class TransactionHandleImpl implements TransactionHandle {
  private final long id;

  TransactionHandleImpl(long id) {
    this.id = id;
  }

  static TransactionHandleImpl withByteBuffers(ByteBuffer[] buffers) {
    return new TransactionHandleImpl(ByteBufferUtils.getLong(buffers));
  }

  @Override
  public ByteBuffer toByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate(ByteBufferUtils.LONG_SIZE);
    buffer.putLong(id).flip();
    return buffer;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TransactionHandleImpl that = (TransactionHandleImpl) o;

    return id == that.id;
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }

  @Override
  public String toString() {
    return "TransactionHandleImpl{" +
            "id=" + id +
            '}';
  }
}
