/*
 * Copyright IBM Corp. 2024, 2025
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

import com.terracottatech.frs.GettableAction;
import com.terracottatech.frs.PutAction;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.action.ActionCodec;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * @author prasa
 */
public class CompactionAction implements GettableAction {
  private final ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry;
  private final ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager;

  private final PutAction delegate;
  private volatile Long lsn;

  CompactionAction(ObjectManager<ByteBuffer, ByteBuffer, ByteBuffer> objectManager,
    ObjectManagerEntry<ByteBuffer, ByteBuffer, ByteBuffer> entry, PutAction action) {
    this.objectManager = objectManager;
    this.entry = entry;
    this.delegate = action;
  }

  @Override
  public ByteBuffer getIdentifier() {
    return delegate.getIdentifier();
  }

  @Override
  public ByteBuffer getKey() {
    return delegate.getKey();
  }

  @Override
  public ByteBuffer getValue() {
    return delegate.getValue();
  }

  @Override
  public long getLsn() {
    return delegate.getLsn();
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return delegate.getInvalidatedLsns();
  }

  @Override
  public void setDisposable(Closeable c) {
    delegate.setDisposable(c);
  }

  @Override
  public void dispose() {
    delegate.dispose();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public int replayConcurrency() {
    return objectManager.replayConcurrency(getIdentifier(), getKey());
  }

  @Override
  public ByteBuffer[] getPayload(ActionCodec codec) {
    return delegate.getPayload(codec);
  }

  @Override
  public void record(long lsn) {
    this.lsn = lsn;
  }

  /**
   * Updates the ObjectManager with the new LSN after this compaction action has been recorded.
   * <p>
   * This method is called after the action has been sequenced (via {@link Action#record(long)})
   * but typically before the compaction entry is released. It ensures that the ObjectManager's
   * state is updated to reflect the compaction operation.
   * <p>
   * This method cannot be called during {@link Action#record(long)} because the compactor
   * typically holds the segment lock at that time. Instead, it's called separately after
   * the action has been sequenced.
   */
  public void updateObjectManager() {
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
