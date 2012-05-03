/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
