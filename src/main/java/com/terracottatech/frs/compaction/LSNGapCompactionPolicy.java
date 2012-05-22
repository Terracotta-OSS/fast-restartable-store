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

  private long compactingCurrentLsn;
  private long compactingLiveSize;
  private long compactedCount;
  private boolean isCompacting = false;

  public LSNGapCompactionPolicy(ObjectManager<?, ?, ?> objectManager, LogManager logManager,
                                Configuration configuration) {
    this.objectManager = objectManager;
    this.logManager = logManager;
    this.minLoad = configuration.getDouble(COMPACTOR_LSNGAP_MIN_LOAD);
    this.maxLoad = configuration.getDouble(COMPACTOR_LSNGAP_MAX_LOAD);
  }

  protected double calculateRatio(long liveEntries, long totalEntries) {
    return ((double) liveEntries) / totalEntries;
  }

  @Override
  public boolean shouldCompact() {
    assert !isCompacting;
    double ratio = calculateRatio(objectManager.size(), logManager.currentLsn() - objectManager.getLowestLsn());
    return ratio <= minLoad;
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
    return ratio <= maxLoad;
  }

  @Override
  public void stoppedCompacting() {
    assert isCompacting;
    isCompacting = false;
  }
}
