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

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.io.IOException;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_AMOUNT;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD;

public class SizeBasedCompactionPolicy implements CompactionPolicy {

  private final IOManager ioManager;
  private final ObjectManager<?, ?, ?> objectManager;
  private final double sizeThreshold;
  private final double compactionPercentage;

  private boolean isCompacting;
  private long entriesToCompact;

  public SizeBasedCompactionPolicy(IOManager ioManager, ObjectManager<?, ?, ?> objectManager,
                                   Configuration configuration) {
    this.ioManager = ioManager;
    this.objectManager = objectManager;
    this.sizeThreshold = configuration.getDouble(COMPACTOR_SIZEBASED_THRESHOLD);
    this.compactionPercentage = configuration.getDouble(COMPACTOR_SIZEBASED_AMOUNT);
  }

  @Override
  public boolean startCompacting() {
    if (isCompacting) {
      throw new IllegalStateException("Already compacting");
    }
    return internalStartCompacting();
  }

  private boolean internalStartCompacting() {
    if (getRatio(objectManager, ioManager) <= sizeThreshold) {
      isCompacting = true;
      entriesToCompact = calculateEntriesToCompact();
      return true;
    } else {
      return false;
    }
  }

  private long calculateEntriesToCompact() {
    return (long) (objectManager.size() * compactionPercentage);
  }

  protected float getRatio(ObjectManager<?, ?, ?> objectManager, IOManager ioManager) {
    try {
      long sizeInBytes = objectManager.sizeInBytes();
      long size = objectManager.size();
      long liveSize = ioManager.getStatistics().getLiveSize();
      long minimumOverhead = LogRegionPacker.getMinimumRecordOverhead() * size;
      long optimallyCompactedSize = sizeInBytes + minimumOverhead;

      return (float) (((double) optimallyCompactedSize) / liveSize);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get log size.", e);
    }
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    if (!isCompacting) {
      throw new IllegalStateException("Compaction is not started.");
    }
    return --entriesToCompact > 0 || internalStartCompacting();
  }

  @Override
  public void stoppedCompacting() {
    if (!isCompacting) {
      throw new IllegalStateException("Compaction is not started.");
    }
    isCompacting = false;
  }
}
