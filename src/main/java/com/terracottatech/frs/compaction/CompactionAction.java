/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
class CompactionAction extends PutAction {
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

  synchronized void updateObjectManager() {
    boolean interrupted = false;
    while (lsn == null) {
      try {
        wait();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    objectManager.updateLsn(entry, lsn);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void replay(long lsn) {
    throw new UnsupportedOperationException("Compaction actions can't be replayed.");
  }
}
