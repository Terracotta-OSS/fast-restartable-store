/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.object;

/**
 *
 * @author Chris Dennis
 */
public interface ObjectManagerSegment<I, K, V> {

  ObjectManagerEntry<I, K, V> acquireCompactionEntry(long ceilingLsn);

  void updateLsn(int hash, ObjectManagerEntry<I, K, V> entry, long newLsn);

  void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry);
  
  Long getLowestLsn();
  
  Long getLsn(int hash, K key);
  
  void put(int hash, K key, V value, long lsn);
  
  void replayPut(int hash, K key, V value, long lsn);
  
  void remove(int hash, K key);

  long size();

  long sizeInBytes();
}
