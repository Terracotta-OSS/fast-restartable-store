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
class MockPutAction<I, K, V> extends MockCompleteKeyAction<I, K> implements Serializable {

  private static final long serialVersionUID = -696424493751601762L;

  private final V value;

  public MockPutAction(I id, K key, V value) {
    super(id, key);
    this.value = value;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return ((ObjectManager<I, K, V>) objManager).put(getId(), getKey(), value, lsn);
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    if (!filter.disallows(this)) {
      ((ObjectManager<I, K, V>) objManager).replayPut(getId(), getKey(), value, lsn);
      return true;
    } else {
      return false;
    }
  }

  public String toString() {
    return "Action: put(" + getId() + ":" + getKey() + ", " + value + ")";
  }
}
