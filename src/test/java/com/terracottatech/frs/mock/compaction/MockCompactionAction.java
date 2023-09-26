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
package com.terracottatech.frs.mock.compaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.mock.MockCompleteKeyAction;
import com.terracottatech.frs.mock.MockPutAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;


/**
 *
 * @author cdennis
 */
public class MockCompactionAction<I, K, V> extends MockCompleteKeyAction<I, K> implements MockAction {

  private final ObjectManagerEntry<I, K, V> entry;
  private Action compacted = null;
  private transient final ObjectManager<I, K, V> objManager;
  
  public MockCompactionAction(ObjectManager<I, K, V> objManager, ObjectManagerEntry<I, K, V> entry) {
    super(entry.getId(), entry.getKey());
    this.entry = entry;
    this.objManager = objManager;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (compacted instanceof MockAction) {
      ((MockAction) compacted).setObjectManager(objManager);
    }
  }

  @Override
  public void record(long lsn) {
    objManager.updateLsn(entry, lsn);
    compacted = new MockPutAction<I, K, V>(objManager, getId(), getKey(), entry.getValue());
  }

  @Override
  public void replay(long lsn) {
    compacted.replay(lsn);
  }

  @Override
  public String toString() {
    return "CompactionAction : compacted=" + compacted; 
  }
}
