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

/**
 * @author tim
 */
class DeleteAction implements Action, DisposableLifecycle {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY = new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
    @Override
    public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                         ActionCodec codec, ByteBuffer[] buffers) {
      return new DeleteAction(objectManager, null, ByteBufferUtils.getFirstNonEmpty(buffers), false);
    }
  };

  private final ObjectManager<ByteBuffer, ?, ?> objectManager;
  private final Compactor compactor;
  private final ByteBuffer id;
  private Closeable        disposable;

  DeleteAction(ObjectManager<ByteBuffer, ?, ?> objectManager, Compactor compactor, ByteBuffer id, boolean recovery) {
    this.objectManager = objectManager;
    this.compactor = compactor;
    this.id = id;

    if (recovery) {
      throw new IllegalStateException("Delete is unsupported during recovery.");
    }
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

  ByteBuffer getId() {
    return id;
  }

  @Override
  public void record(long lsn) {
    objectManager.delete(id);
    compactor.compactNow();
  }

  @Override
  public void replay(long lsn) {
    // nothing to do on replay
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[] { id.slice() };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DeleteAction that = (DeleteAction) o;

    return id.equals(that.id);
  }

  @Override
  public int hashCode() {
    return id != null ? id.hashCode() : 0;
  }
}
