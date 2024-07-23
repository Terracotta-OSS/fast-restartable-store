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

import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockRemoveAction<I, K> extends MockCompleteKeyAction<I, K> implements Serializable, MockAction,
        InvalidatingAction {

  private final long invalidatedLsn;
  private transient ObjectManager<I, K, ?> objManager;
  
  public MockRemoveAction(ObjectManager<I, K, ?> objManager, I id, K key) {
    super(id, key);
    this.objManager = objManager;
    invalidatedLsn = objManager.getLsn(id, key);
  }

  @Override
  public void record(long lsn) {
     objManager.remove(getId(), getKey());
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void replay(long lsn) {
    //no-op
  }
  
  public String toString() {
    return "Action: remove(" + getId() + ":" + getKey() + ")";
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, K, ?>) objManager;
  }
}
