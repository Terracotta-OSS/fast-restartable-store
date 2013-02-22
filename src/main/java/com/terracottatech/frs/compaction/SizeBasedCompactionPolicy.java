package com.terracottatech.frs.compaction;

import com.terracottatech.frs.config.Configuration;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.io.IOException;

import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_AMOUNT;
import static com.terracottatech.frs.config.FrsProperty.COMPACTOR_SIZEBASED_THRESHOLD;

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
    if (isCompacting) {
      throw new IllegalStateException("Already compacting");
    }
    return internalStartCompacting();
  }

  private boolean internalStartCompacting() {
    if (getRatio() <= sizeThreshold) {
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

  private float getRatio() {
    try {
      return objectManager.sizeInBytes() / ioManager.getStatistics().getLiveSize();
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
