/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.ActionFactory;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
class CompactionAction extends PutAction {
  public static final ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer> FACTORY =
          new ActionFactory<ByteBuffer, ByteBuffer, ByteBuffer>() {
            @Override
            public Action create(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
                                 ActionCodec codec, ByteBuffer[] buffers) {
                return PutAction.FACTORY.create(objectManager, codec, buffers);
            }
          };

  private final ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry;
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;

  private Long lsn;

  CompactionAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
    super(objectManager, null, entry.getId(), entry.getKey(), entry.getValue(), entry.getLsn());
    this.objectManager = objectManager;
    this.entry = entry;
  }

  @Override
  public void record(long lsn) {
    synchronized (this) {
      this.lsn = lsn;
      notifyAll();
    }
  }

  synchronized void updateObjectManager() throws InterruptedException {
    while (lsn == null) {
        wait();
    }
    objectManager.updateLsn(entry, lsn);
  }

  @Override
  public void replay(long lsn) {
    throw new UnsupportedOperationException("Compaction actions can't be replayed.");
  }
}
