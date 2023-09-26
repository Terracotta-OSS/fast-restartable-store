/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.compaction.Compactor;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.ByteBufferUtils;
import java.io.Closeable;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 * @author tim
 */
public class PutAction implements GettableAction {
  /* PutAction.getPayload
  4 bytes - PutAction.idByteCount
  4 bytes - PutAction.keyByteCount
  4 bytes - PutAction.valueByteCount
  8 bytes - PutAction.invalidatedLsn
  */
  public static final long PUT_ACTION_OVERHEAD = 20L;

  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
              int idLength = ByteBufferUtils.getInt(buffers);
              int keyLength = ByteBufferUtils.getInt(buffers);
              int valueLength = ByteBufferUtils.getInt(buffers);
              long invalidatedLsn = ByteBufferUtils.getLong(buffers);
              ByteBuffer id = ByteBufferUtils.getBytes(idLength, buffers);
              ByteBuffer key = ByteBufferUtils.getBytes(keyLength, buffers);
              ByteBuffer value = ByteBufferUtils.getBytes(valueLength, buffers);
              return new PutAction(objectManager, null, id, key, value, invalidatedLsn);
            }
          };

  private static final int HEADER_SIZE =
          ByteBufferUtils.INT_SIZE * 3 + ByteBufferUtils.LONG_SIZE;

  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;
  private final ByteBuffer                                        id;
  private final ByteBuffer                                        key;
  private final ByteBuffer                                        value;
  private final Compactor                                         compactor;

  private long                                                    markedLsn;
  private long                                                    invalidatedLsn;
  private Closeable                                              disposable;

  PutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, Compactor compactor, ByteBuffer id,
            ByteBuffer key, ByteBuffer value, boolean recovery) {
    this(objectManager, compactor, id, key, value, objectManager.getLsn(id, key));
    if (invalidatedLsn == -1L && recovery) {
      throw new IllegalStateException(
              "Put over an unrecovered key is unsupported during recovery.");
    }
  }

  protected PutAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, Compactor compactor, ByteBuffer id,
                    ByteBuffer key, ByteBuffer value, long invalidatedLsn) {
    this.objectManager = objectManager;
    this.compactor = compactor;
    this.id = id;
    this.key = key;
    this.value = value;
    this.invalidatedLsn = invalidatedLsn;
  }

  @Override
  public ByteBuffer getIdentifier() {
    return id;
  }

  @Override
  public ByteBuffer getKey() {
    return key;
  }

  @Override
  public ByteBuffer getValue() {
    return value;
  }

    @Override
    public long getLsn() {
        return markedLsn;
    }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void setDisposable(Closeable c) {
    disposable = c;
  }

  @Override
  public void dispose() {
    try {
      this.close();
    } catch ( IOException ioe ) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void close() throws IOException {
    if ( disposable != null ) {
      disposable.close();
      disposable = null;
    }
  }

  @Override
  public void record(long lsn) {
    markedLsn = lsn;
    objectManager.put(getIdentifier(), getKey(), getValue(), lsn);
    if (invalidatedLsn != -1) {
      compactor.generatedGarbage(invalidatedLsn);
    }
  }
  
  @Override
  public void replay(long lsn) {
    objectManager.replayPut(getIdentifier(), getKey(), getValue(), lsn);
  }

  @Override
  public int replayConcurrency() {
    return objectManager.replayConcurrency(getIdentifier(), getKey());
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
    header.putInt(id.remaining());
    header.putInt(key.remaining());
    header.putInt(value.remaining());
    header.putLong(invalidatedLsn).flip();
    return new ByteBuffer[]{header, id.slice(), key.slice(), value.slice()};
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PutAction putAction = (PutAction) o;

    return id.equals(putAction.id) && key.equals(putAction.key) && value.equals(
            putAction.value) && invalidatedLsn == putAction.invalidatedLsn;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (key != null ? key.hashCode() : 0);
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}
