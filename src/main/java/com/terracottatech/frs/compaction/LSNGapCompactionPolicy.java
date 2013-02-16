package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MAX_LOAD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_MIN_LOAD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_LSNGAP_WINDOW_SIZE;

/**
 * @author tim
 */
public class LSNGapCompactionPolicy implements CompactionPolicy {
  private final ObjectManager<?, ?, ?> objectManager;
  private final LogManager logManager;
  private final double minLoad;
  private final double maxLoad;
  private final int windowSize;

  private long compactedCount;
  private long liveSize;
  private long currentLsn;
  private int windowCount;
  private boolean isCompacting = false;

  public LSNGapCompactionPolicy(ObjectManager<?, ?, ?> objectManager, LogManager logManager,
                                Configuration configuration) {
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.minLoad = configuration.getDouble(COMPACTOR_LSNGAP_MIN_LOAD);
    this.maxLoad = configuration.getDouble(COMPACTOR_LSNGAP_MAX_LOAD);
    this.windowSize = configuration.getInt(COMPACTOR_LSNGAP_WINDOW_SIZE);
  }

  protected float calculateRatio(long liveEntries, long totalEntries) {
    return ((float) liveEntries) / totalEntries;
  }

  @Override
  public boolean startCompacting() {
    if (isCompacting) {
      throw new IllegalStateException("Compaction is already started.");
    }
    liveSize = objectManager.size();
    currentLsn = logManager.currentLsn();
    long lowestLsn = objectManager.getLowestLsn();
    float ratio = calculateRatio(liveSize, currentLsn - lowestLsn);
    if (ratio <= minLoad) {
      compactedCount = 0;
      windowCount = 0;
      isCompacting = true;
      return true;
    } else {
      return false;
    }
  }

  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    if (!isCompacting) {
      throw new IllegalStateException("Compaction is not running.");
    }
    compactedCount++;
    if (compactedCount >= liveSize) {
      return false;
    }
    // The way this termination condition works is by checking the approximate length
    // of the LSN span against the live object count. As entries are compacted over, the
    // window shrinks in length until it equals the live object count.
    // Calculation is simplified from:
    // liveSize / (currentLsn + compactedCount - entry.getLsn()) <= ratio

    double estimatedRatio = estimateRatio(entry.getLsn());
    if (estimatedRatio <= maxLoad || estimatedRatio > 1.0) {
      windowCount = 0;
      return true;
    } else {
      if (++windowCount >= windowSize) {
        long officialLowestLsn = objectManager.getLowestLsn();
        logManager.updateLowestLsn(officialLowestLsn); // might as well send it through to the log manager.
        estimatedRatio = estimateRatio(officialLowestLsn);
        if (estimatedRatio <= maxLoad) {
          windowCount = 0;
          return true;
        } else {
          return false;
        }
      } else {
        return true;
      }
    }
  }

  private double estimateRatio(long minLsn) {
    return ((double) liveSize) / (compactedCount + currentLsn - minLsn);
  }

  @Override
  public void stoppedCompacting() {
    if (!isCompacting) {
      throw new IllegalStateException("Compaction is not running.");
    }
    isCompacting = false;
  }
}
