/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.util.TestUtils;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public class MapActionFactory {
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;

  public MapActionFactory(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager) {
    this.objectManager = objectManager;
  }

  public Action put(int i, int k, int v) {
    return new PutAction(objectManager, TestUtils.byteBufferWithInt(i),
                         TestUtils.byteBufferWithInt(k), TestUtils.byteBufferWithInt(v));
  }

  public Action remove(int i, int k) {
    return new RemoveAction(objectManager, TestUtils.byteBufferWithInt(i),
                            TestUtils.byteBufferWithInt(k));
  }

  public Action delete(int i) {
    return new DeleteAction(objectManager, TestUtils.byteBufferWithInt(i));
  }
}
