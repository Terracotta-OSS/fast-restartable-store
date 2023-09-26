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
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class MockDeleteAction<I> implements MockAction, Serializable {

  private final I id;
  private transient ObjectManager<I, ?, ?> objManager;

  public MockDeleteAction(ObjectManager<I, ?, ?> objManager, I id) {
    this.id = id;
    this.objManager = objManager;
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, ?, ?>) objManager;
  }

  @Override
  public void record(long lsn) {
    objManager.delete(id);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  public I getId() {
    return id;
  }
}
