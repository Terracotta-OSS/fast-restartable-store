/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
