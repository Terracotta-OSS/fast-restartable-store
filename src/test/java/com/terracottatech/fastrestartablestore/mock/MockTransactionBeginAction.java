/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author cdennis
 */
class MockTransactionBeginAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionBeginAction(long id) {
    this.id = id;
  }
  
  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return -1;
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    filter.removeRule(new MockAllowTransactionRule(id));
    return false;
  }

  public String toString() {
    return "Action: beginTransaction(" + id + ")";
  }

  public Collection<Lock> lock(List<ReadWriteLock> locks) {
    return Collections.emptyList();
  }
}
