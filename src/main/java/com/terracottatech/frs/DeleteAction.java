/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
