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
package com.terracottatech.frs.mock.transaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 * 
 * @author cdennis
 */
public class MockTransactionalAction implements MockAction, Serializable,
        InvalidatingAction {
  private static final long serialVersionUID = 1L;
  private final long id;
  private final Action embedded;

  public MockTransactionalAction(long id, Action action) {
    this.id = id;
    this.embedded = action;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (embedded instanceof MockAction) {
      ((MockAction) embedded).setObjectManager(objManager);
    }
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    if (embedded instanceof InvalidatingAction) {
      return ((InvalidatingAction) embedded).getInvalidatedLsns();
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public void record(long lsn) {
    embedded.record(lsn);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  public String toString() {
    return "Transactional[id=" + id + "] " + embedded;
  }

  long getId() {
    return id;
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return new ByteBuffer[0];
  }

  public Action getEmbeddedAction() {
    return embedded;
  }
}
