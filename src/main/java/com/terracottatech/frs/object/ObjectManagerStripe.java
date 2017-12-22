package com.terracottatech.frs.object;

import java.util.Collection;

public interface ObjectManagerStripe<I, K, V> {

  Long getLowestLsn();

  Long getLsn(K key);

  void put(K key, V value, long lsn);

  void remove(K key);

  void delete();
  
  void replayPut(K key, V value, long lsn);

  default int replayConcurrency(K key) {
    return 1;
  }

  Collection<ObjectManagerSegment<I, K, V>> getSegments();

  void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn);

  void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry);

  long size();

  long sizeInBytes();
}
