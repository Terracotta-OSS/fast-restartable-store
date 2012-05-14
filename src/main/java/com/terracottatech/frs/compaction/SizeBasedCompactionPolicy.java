package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public class SizeBasedCompactionPolicy implements CompactionPolicy {
  private static final String SIZE_THRESHOLD_KEY = "compactor.sizeBased.threshold";
  private static final String COMPACTION_PERCENTAGE_KEY = "compactor.sizeBased.amount";

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
    this.sizeThreshold = configuration.getDouble(SIZE_THRESHOLD_KEY);
    this.compactionPercentage = configuration.getDouble(COMPACTION_PERCENTAGE_KEY);
  }

  @Override
  public boolean shouldCompact() {
    try {
      double ratio = ((double) objectManager.sizeInBytes()) / ioManager.getStatistics().getLiveSize();
      return ratio <= sizeThreshold;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get data size.", e);
    }
  }

  @Override
  public void startedCompacting() {
    assert !isCompacting;
    isCompacting = true;
    entriesToCompact = (long) (objectManager.size() * compactionPercentage);
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    assert isCompacting;
    return entriesToCompact-- > 0;
  }

  @Override
  public void stoppedCompacting() {
    assert isCompacting;
    isCompacting = false;
  }
}
