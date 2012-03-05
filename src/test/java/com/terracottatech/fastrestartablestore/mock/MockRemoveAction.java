/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import java.io.Serializable;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
class MockRemoveAction<I, K> implements Action, Serializable {

  private final I id;
  private final K key;
  
  public MockRemoveAction(I id, K key) {
    this.id = id;
    this.key = key;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return ((ObjectManager<I, K, ?>) objManager).recordRemove(id, key, lsn);
  }

  @Override
  public void replay(ObjectManager<?, ?, ?> objManager, long lsn) {
    ((ObjectManager<I, K, ?>) objManager).replayRemove(id, key, lsn);
  }
  
  public String toString() {
    return "Action: remove(" + key + ")";
  }

}
