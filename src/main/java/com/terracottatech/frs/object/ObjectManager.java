/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 * @author cdennis
 */
public interface ObjectManager<I, K, V> {
  
  long getLsn(I id, K key);
  
  /*
   * XXX : do we want to have V here or not - should we make a decision on 
   * wrapping versus embedding of the final library.
   */
  void put(I id, K key, V value, long lsn);
  
  void delete(I id);
  
  void remove(I id, K key);

  /**
   * Replay a put. For purposes of handling evictions during recovery, also returns
   * a list of LSNs for entries that have been evicted.
   *
   * @param id identifier
   * @param key key
   * @param value value
   * @param lsn lsn
   */
  void replayPut(I id, K key, V value, long lsn);

  /**
   * Returns a number that determines the parallelism of replay during recovery.
   *
   * By default, there is no recovery parallelism.
   *
   * @param id identifier
   * @param key key
   * @return an integer indicating parallelism for this entry
   */
  default int replayConcurrency(I id, K key) {
    return 1;
  }

  /**
   * Get and lock an entry to be compacted in the object manager
   *
   * @param ceilingLsn highest LSN eligible for compaction
   * @return entry to be compacted. null if the object manager is empty.
   */
  ObjectManagerEntry<I, K, V> acquireCompactionEntry(long ceilingLsn);

  /**
   * Release the lock entry after compaction is complete.
   *
   * @param entry compacted entry
   */
  void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry);

  void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn);

  /**
   * Get an approximately correct count of the number of live objects in the system.
   *
   * @return number of live objects
   */
  long size();

  /**
   * Get the occupied byte size of this object manager.
   *
   * @return size in bytes
   */
  long sizeInBytes();
  /**
   * Returns an estimate of the lowest live lsn in the system.
   * <p>
   * It is acceptable to underestimate this value but not to overestimate it.
   * Since the estimated quantity is monotonically increasing this means it is
   * acceptable to return an out-of-date estimate here.
   *
   * @return an estimate of the oldest live lsn, {@code -1} if no objects are available for query.
   */
  long getLowestLsn();
}
