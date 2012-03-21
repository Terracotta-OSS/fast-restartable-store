/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

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
  public long getPreviousLsn() {
    return objManager.getLsn(getId(), getKey());
  }

  @Override
  public void record(long lsn) {
    objManager.put(getId(), getKey(), value, lsn);
  }

  @Override
  public void replay(long lsn) {
    objManager.replayPut(getId(), getKey(), value, lsn);
  }

  public String toString() {
    return "Action: put(" + getId() + ":" + getKey() + ", " + value + ")";
  }
}
