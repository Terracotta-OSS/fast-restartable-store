/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
    ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
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
