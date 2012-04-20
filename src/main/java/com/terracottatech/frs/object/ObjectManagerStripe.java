package com.terracottatech.frs.object;

import java.util.Collection;

public interface ObjectManagerStripe<I, K, V> {

  I identifier();
  
  Long getLowestLsn();

  long getLsn(K key);

  void put(K key, V value, long lsn);

  void remove(K key);

  void replayPut(K key, V value, long lsn);

  V replaceLsn(K key, long newLsn);

  Collection<ObjectManagerSegment<I, K, V>> getSegments();
}
