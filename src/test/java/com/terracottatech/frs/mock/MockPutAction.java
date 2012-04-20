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
 * @author cdennis
 */
public class MockPutAction<I, K, V> extends MockCompleteKeyAction<I, K> implements
        Serializable, MockAction,
        InvalidatingAction {

  private static final long serialVersionUID = -696424493751601762L;

  private final     V                      value;
  private final     long                   invalidatedLsn;
  private transient ObjectManager<I, K, V> objManager;

  public MockPutAction(ObjectManager<I, K, V> objManager, I id, K key, V value) {
    super(id, key);
    this.value = value;
    this.objManager = objManager;
    // This isn't exactly correct in terms of lock contexts, but it's close enough for the
    // mock.
    invalidatedLsn = objManager.getLsn(id, key);
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, K, V>) objManager;
  }

  @Override
  public Set<Long> getInvalidatedLsns() {
    return Collections.singleton(invalidatedLsn);
  }

  @Override
  public void record(long lsn) {
    objManager.put(getId(), getKey(), value, lsn);
  }

  @Override
  public Set<Long> replay(long lsn) {
    return objManager.replayPut(getId(), getKey(), value, lsn);
  }

  public String toString() {
    return "Action: put(" + getId() + ":" + getKey() + ", " + value + ")";
  }
}
