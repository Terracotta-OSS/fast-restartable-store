package com.terracottatech.frs.compaction;

import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public class NoCompactionPolicy implements CompactionPolicy {

  @Override
  public boolean startCompacting() {
    return false;
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    throw new AssertionError("Should not be compacting with a NoCompactionPolicy.");
  }

  @Override
  public void stoppedCompacting() {
    throw new AssertionError("Should not be compacting with a NoCompactionPolicy.");
  }
}
