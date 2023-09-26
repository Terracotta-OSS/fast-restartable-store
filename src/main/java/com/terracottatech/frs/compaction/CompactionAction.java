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
package com.terracottatech.frs.compaction;

import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.nio.ByteBuffer;

/**
 * @author tim
 */
class CompactionAction extends PutAction {
  private final ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry;
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;

  private volatile Long lsn;

  CompactionAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager, ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry) {
    super(objectManager, null, entry.getId(), entry.getKey(), entry.getValue(), entry.getLsn());
    this.objectManager = objectManager;
    this.entry = entry;
  }

  @Override
  public void record(long lsn) {
    this.lsn = lsn;
  }

  void updateObjectManager() {
    while (lsn == null) {
      // Just spin, this shouldn't take long.
    }
    objectManager.updateLsn(entry, lsn);
  }

  @Override
  public void replay(long lsn) {
    throw new UnsupportedOperationException("Compaction actions can't be replayed.");
  }
}
