/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.object.ObjectManager;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public interface ActionFactory<I, K, V> {
  Action create(ObjectManager<I, K, V> objectManager,
                ActionCodec codec, ByteBuffer[] buffers);
}
