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
class MockTransactionCommitAction implements Action, Serializable {

  private final long id;
  
  public MockTransactionCommitAction(long id) {
    this.id = id;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    return -1;
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    filter.addRule(new MockAllowTransactionRule(id));
    return false;
  }

  public String toString() {
    return "Action: commitTransaction(" + id + ")";
  }

  public long getId() {
   return id;
  }

  @Override
  public Collection<Lock> lock(List<ReadWriteLock> locks) {
    return Collections.emptyList();
  }
}
