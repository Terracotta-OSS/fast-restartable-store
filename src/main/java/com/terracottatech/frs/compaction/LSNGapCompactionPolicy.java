package com.terracottatech.frs.compaction;

import com.terracottatech.frs.log.LogManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public class LSNGapCompactionPolicy implements CompactionPolicy {
  private static final double MINIMUM_LOAD = 0.30;
  private static final double MAXIMUM_LOAD = 0.60;

  private final ObjectManager<?, ?, ?> objectManager;
  private final LogManager logManager;

  private long compactingCurrentLsn;
  private long compactingLiveSize;
  private long compactedCount;
  private boolean isCompacting = false;

  public LSNGapCompactionPolicy(ObjectManager<?, ?, ?> objectManager, LogManager logManager) {
    this.objectManager = objectManager;
    this.logManager = logManager;
  }

  protected double calculateRatio(long liveEntries, long totalEntries) {
    return ((double) liveEntries) / totalEntries;
  }

  @Override
  public boolean shouldCompact() {
    assert !isCompacting;
    double ratio = calculateRatio(objectManager.size(), logManager.currentLsn() - objectManager.getLowestLsn());
    return ratio <= MINIMUM_LOAD;
  }

  @Override
  public void startedCompacting() {
    assert !isCompacting;
    compactingCurrentLsn = logManager.currentLsn();
    compactedCount = 0;
    compactingLiveSize = objectManager.size();
    isCompacting = true;
  }

  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    assert isCompacting;
    compactedCount++;
    // The way this termination condition works is by checking the approximate length
    // of the LSN span against the live object count. As entries are compacted over, the
    // window shrinks in length until it equals the live object count.
    double ratio = calculateRatio(compactingLiveSize, compactingCurrentLsn + compactedCount - entry.getLsn());
    return ratio <= MAXIMUM_LOAD;
  }

  @Override
  public void stoppedCompacting() {
    assert isCompacting;
    isCompacting = false;
  }
}
