package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_AMOUNT;

/**
 * @author tim
 */
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
