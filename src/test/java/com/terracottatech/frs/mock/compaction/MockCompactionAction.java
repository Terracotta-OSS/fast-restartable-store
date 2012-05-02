/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.compaction;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.mock.MockCompleteKeyAction;
import com.terracottatech.frs.mock.MockPutAction;
import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.object.ObjectManager;
import com.terracottatech.frs.object.ObjectManagerEntry;

import java.util.Collections;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public class MockCompactionAction<I, K, V> extends MockCompleteKeyAction<I, K> implements MockAction {

  private final ObjectManagerEntry<I, K, V> entry;
  private Action compacted = null;
  private transient final ObjectManager<I, K, V> objManager;
  
  public MockCompactionAction(ObjectManager<I, K, V> objManager, ObjectManagerEntry<I, K, V> entry) {
    super(entry.getId(), entry.getKey());
    this.entry = entry;
    this.objManager = objManager;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (compacted instanceof MockAction) {
      ((MockAction) compacted).setObjectManager(objManager);
    }
  }

  @Override
  public void record(long lsn) {
    objManager.updateLsn(entry, lsn);
    compacted = new MockPutAction<I, K, V>(objManager, getId(), getKey(), entry.getValue());
  }

  @Override
  public void replay(long lsn) {
    compacted.replay(lsn);
  }

  @Override
  public String toString() {
    return "CompactionAction : compacted=" + compacted; 
  }
}
