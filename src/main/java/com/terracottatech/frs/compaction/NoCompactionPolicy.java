package com.terracottatech.frs.compaction;

import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public class NoCompactionPolicy implements CompactionPolicy {
  @Override
  public boolean shouldCompact() {
    return false;
  }

  @Override
  public void startedCompacting() {
  }

  @Override
  public boolean compacted(ObjectManagerEntry<?, ?, ?> entry) {
    return false;
  }

  @Override
  public void stoppedCompacting() {
  }
}
