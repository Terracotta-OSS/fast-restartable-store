/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

import java.io.Serializable;

/**
 *
 * @author cdennis
 */
class MockRemoveAction<I, K> extends MockCompleteKeyAction<I, K> implements Serializable {

  public MockRemoveAction(I id, K key) {
    super(id, key);
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return ((ObjectManager<I, K, ?>) objManager).remove(getId(), getKey());
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    if (!filter.disallows(this)) {
      return true;
    } else {
      return false;
    }
  }
  
  public String toString() {
    return "Action: remove(" + getId() + ":" + getKey() + ")";
  }
}
