package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MAX_LOAD;

/**
 * @author tim
 */
public class LSNGapCompactionPolicy implements CompactionPolicy {
  private final ObjectManager<?, ?, ?> objectManager;
  private final LogManager logManager;
  private final double minLoad;
  private final double maxLoad;

  private long compactedCount;
  private long compactionStopCount;
  private boolean isCompacting = false;

  public LSNGapCompactionPolicy(ObjectManager<?, ?, ?> objectManager, LogManager logManager,
                                Configuration configuration) {
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.minLoad = configuration.getDouble(COMPACTOR_LSNGAP_MIN_LOAD);
    this.maxLoad = configuration.getDouble(COMPACTOR_LSNGAP_MAX_LOAD);
  }

  protected float calculateRatio(long liveEntries, long totalEntries) {
    return ((float) liveEntries) / totalEntries;
  }

  @Override
  public boolean startCompacting() {
    if (isCompacting) {
      throw new AssertionError("Compaction is already started.");
    }
    long liveSize = objectManager.size();
    long currentLsn = logManager.currentLsn();
    long lowestLsn = objectManager.getLowestLsn();
    float ratio = calculateRatio(liveSize, currentLsn - lowestLsn);
    if (ratio <= minLoad) {
      compactionStopCount = ((long) (liveSize / maxLoad)) - currentLsn;
      compactedCount = 0;
      isCompacting = true;
      return true;
    } else {
      return false;
    }
  }

  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    if (!isCompacting) {
      throw new AssertionError("Compaction is not running.");
    }
    compactedCount++;
    // The way this termination condition works is by checking the approximate length
    // of the LSN span against the live object count. As entries are compacted over, the
    // window shrinks in length until it equals the live object count.
    // Calculation is simplified from:
    // liveSize / (currentLsn + compactedCount - entry.getLsn()) <= ratio
    return compactionStopCount <= compactedCount - entry.getLsn();
  }

  @Override
  public void stoppedCompacting() {
    if (!isCompacting) {
      throw new AssertionError("Compaction is not running.");
    }
    isCompacting = false;
  }
}
