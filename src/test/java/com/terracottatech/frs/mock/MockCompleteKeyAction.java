/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.CompleteKey;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public abstract class MockCompleteKeyAction<I, K> implements Action, Serializable {
  private final I id;
  private final K key;
  
  public MockCompleteKeyAction() {
    // Get rid of this
    id = null;
    key = null;
  }
  
  public MockCompleteKeyAction(CompleteKey<I, K> completeKey) {
    this(completeKey.getId(), completeKey.getKey());
  }
  
  public MockCompleteKeyAction(I id, K key) {
    this.id = id;
    this.key = key;
  }
  
  public I getId() {
    return id;
  }

  protected K getKey() {
    return key;
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }
}
