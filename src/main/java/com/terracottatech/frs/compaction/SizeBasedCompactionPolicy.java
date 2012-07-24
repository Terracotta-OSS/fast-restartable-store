package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.io.IOException;

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
  public boolean startCompacting() {
    float ratio;
    try {
      ratio = objectManager.sizeInBytes() / ioManager.getStatistics().getLiveSize();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get log size.", e);
    }
    if (ratio <= sizeThreshold) {
      isCompacting = true;
      entriesToCompact = (long) (objectManager.size() * compactionPercentage);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    if (!isCompacting) {
      throw new AssertionError("Compaction is not started.");
    }
    return entriesToCompact-- > 0;
  }

  @Override
  public void stoppedCompacting() {
    if (!isCompacting) {
      throw new AssertionError("Compaction is not started.");
    }
    isCompacting = false;
  }
}
