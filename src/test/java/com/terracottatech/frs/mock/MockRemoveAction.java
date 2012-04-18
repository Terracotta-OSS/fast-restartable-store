/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock;

import com.terracottatech.frs.action.InvalidatingAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;
import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockRemoveAction<I, K> extends MockCompleteKeyAction<I, K> implements Serializable, MockAction,
        InvalidatingAction {

  private final long invalidatedLsn;
  private transient ObjectManager<I, K, ?> objManager;
  
  public MockRemoveAction(ObjectManager<I, K, ?> objManager, I id, K key) {
    super(id, key);
    this.objManager = objManager;
    invalidatedLsn = objManager.getLsn(id, key);
  }

  @Override
  public void record(long lsn) {
     objManager.remove(getId(), getKey());
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
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
