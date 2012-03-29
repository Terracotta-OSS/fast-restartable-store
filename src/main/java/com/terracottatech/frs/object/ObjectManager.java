/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 * @author cdennis
 */
public interface ObjectManager<I, K, V> {

  /**
   * Returns an estimate of the lowest live lsn in the system.
   * <p>
   * It is acceptable to underestimate this value but not to overestimate it.
   * Since the estimated quantity is monotonically increasing this means it is
   * acceptable to return an out-of-date estimate here.
   *
   * @return an estimate of the oldest live lsn, {@code -1} if none.
   */
  long getLowestLsn();
  
  long getLsn(I id, K key);
  
  /*
   * XXX : do we want to have V here or not - should we make a decision on 
   * wrapping versus embedding of the final library.
   */
  void put(I id, K key, V value, long lsn);
  
  void delete(I id);
  
  void remove(I id, K key);

  void replayPut(I id, K key, V value, long lsn);

  /**
   * Return a complete key suitable for compaction.
   * 
   * @return a key to be compacted.
   */
  CompleteKey<I, K> getCompactionKey();
  
  V replaceLsn(I id, K key, long newLsn);
}
