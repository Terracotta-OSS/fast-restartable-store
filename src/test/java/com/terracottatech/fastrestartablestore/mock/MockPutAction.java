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
class MockPutAction<I, K, V> extends MockCompleteKeyAction<I, K> implements Serializable, MockAction {

  private static final long serialVersionUID = -696424493751601762L;

  private final V value;
  private transient ObjectManager<I, K, V> objManager;

  public MockPutAction(ObjectManager<I, K, V> objManager, I id, K key, V value) {
    super(id, key);
    this.value = value;
    this.objManager = objManager;
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, K, V>) objManager;
  }

  @Override
  public long getLsn() {
    return objManager.getLsn(getId(), getKey());
  }

  @Override
  public void record(long lsn) {
    objManager.put(getId(), getKey(), value, lsn);
  }

  @Override
  public boolean replay(ReplayFilter filter, long lsn) {
    if (!filter.disallows(this)) {
      objManager.replayPut(getId(), getKey(), value, lsn);
      return true;
    } else {
      return false;
    }
  }

  public String toString() {
    return "Action: put(" + getId() + ":" + getKey() + ", " + value + ")";
  }
}
