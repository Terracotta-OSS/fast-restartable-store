/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

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
  public void replay(ObjectManager<?, ?, ?> objManager, long lsn) {
    ((ObjectManager<I, K, V>) objManager).replayPut(id, key, value, lsn);
  }

  public String toString() {
    return "Action: put(" + key + ", " + value + ")";
  }
}
