package com.terracottatech.frs.compaction;

import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public class SizeBasedCompactionPolicy implements CompactionPolicy {
  private static final double MIN_THRESHOLD = 0.50;
  private static final double COMPACTION_PERCENTAGE = 0.05;

  private final IOManager ioManager;
  private final ObjectManager<?, ?, ?> objectManager;

  private boolean isCompacting;
  private long entriesToCompact;

  public SizeBasedCompactionPolicy(IOManager ioManager, ObjectManager<?, ?, ?> objectManager) {
    this.ioManager = ioManager;
    this.objectManager = objectManager;
  }

  @Override
  public boolean shouldCompact() {
    try {
      double ratio = ((double) objectManager.sizeInBytes()) / ioManager.getStatistics().getLiveSize();
      return ratio <= MIN_THRESHOLD;
    } catch (Exception e) {
      throw new RuntimeException("Failed to get data size.", e);
    }
  }

  @Override
  public void startedCompacting() {
    assert !isCompacting;
    isCompacting = true;
    entriesToCompact = (long) (objectManager.size() * COMPACTION_PERCENTAGE);
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
