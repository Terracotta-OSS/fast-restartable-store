package com.terracottatech.frs.compaction;

import com.terracottatech.frs.object.ObjectManagerEntry;

/**
 * @author tim
 */
public interface CompactionPolicy {
  /**
   * Called to check if compaction should start.
   *
   * @return whether or not compaction should begin
   */
  boolean shouldCompact();

  /**
   * Notify the {@link CompactionPolicy} that compaction has begun
   */
  void startedCompacting();

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
