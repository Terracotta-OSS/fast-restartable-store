/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
public interface ActionCodec {

  void registerAction(int collectionId, int actionId, Class<? extends Action> actionClass);

  Action decode(ByteBuffer[] buffer) throws ActionDecodeException;

  ByteBuffer[] encode(Action action);
}
