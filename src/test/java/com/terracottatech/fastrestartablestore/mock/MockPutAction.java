/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

import java.io.Serializable;

/**
 * 
 * @author cdennis
 */
class MockPutAction<I, K, V> implements Action, Serializable {

  private static final long serialVersionUID = -696424493751601762L;

  private final I id;
  private final K key;
  private final V value;

  public MockPutAction(I id, K key, V value) {
    this.id = id;
    this.key = key;
    this.value = value;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return ((ObjectManager<I, K, V>) objManager).recordPut(id, key, lsn);
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    if (!filter.disallows(this)) {
      ((ObjectManager<I, K, V>) objManager).replayPut(id, key, value, lsn);
      return true;
    } else {
      return false;
    }
  }

  public String toString() {
    return "Action: put(" + key + ", " + value + ")";
  }
  
  I getId() {
    return id;
  }
}
