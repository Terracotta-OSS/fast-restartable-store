package com.terracottatech.frs.compaction;

import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public interface CompactionPolicy {
  /**
   * Query this {@link CompactionPolicy} for whether or not compaction should begin. If so
   * start compaction.
   *
   * @return true to indicate that the compactor should compact
   */
  boolean startCompacting();

  /**
   * Polled during compaction to check if compaction should continue.
   *
   * @return whether compaction should continue
   */
  boolean compacted(ObjectManagerEntry<?, ?, ?> entry);

  /**
   * Notify this {@link CompactionPolicy} that compaction has finished.
   */
  void stoppedCompacting();
}
