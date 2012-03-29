/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;
import java.io.Serializable;

/**
 *
 * @author cdennis
 */
public class MockRemoveAction<I, K> extends MockCompleteKeyAction<I, K> implements Serializable, MockAction {

  private transient ObjectManager<I, K, ?> objManager;
  
  public MockRemoveAction(ObjectManager<I, K, ?> objManager, I id, K key) {
    super(id, key);
    this.objManager = objManager;
  }

  @Override
  public long getPreviousLsn() {
    return objManager.getLsn(getId(), getKey());
  }

  @Override
  public void record(long lsn) {
     objManager.remove(getId(), getKey());
  }

  @Override
  public void replay(long lsn) {
    //no-op
  }
  
  public String toString() {
    return "Action: remove(" + getId() + ":" + getKey() + ")";
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, K, ?>) objManager;
  }
}
