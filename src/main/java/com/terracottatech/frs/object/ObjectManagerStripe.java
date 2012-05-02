package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.Set;

public interface ObjectManagerStripe<I, K, V> {

  I identifier();
  
  Long getLowestLsn();

  Long getLsn(K key);

  void put(K key, V value, long lsn);

  void remove(K key);

  void replayPut(K key, V value, long lsn);

  Collection<ObjectManagerSegment<I, K, V>> getSegments();

  void updateLsn(ObjectManagerEntry<I, K, V> entry, long newLsn);

  void releaseCompactionEntry(ObjectManagerEntry<I, K, V> entry);

  long size();
}
