package com.terracottatech.frs.object;

import java.util.Collection;
import java.util.Set;

public interface ObjectManagerStripe<I, K, V> {

  I identifier();
  
  Long getLowestLsn();

  Long getLsn(K key);

  void put(K key, V value, long lsn);

  void remove(K key);

  Set<Long> replayPut(K key, V value, long lsn);

  V replaceLsn(K key, long newLsn);

  Collection<ObjectManagerSegment<I, K, V>> getSegments();

  long size();
}
