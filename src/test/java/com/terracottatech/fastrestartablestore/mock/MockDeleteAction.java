package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class MockDeleteAction<I> implements Action {

  private final I id;
  
  public MockDeleteAction(I id) {
    this.id = id;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    ((ObjectManager<I, ?, ?>) objManager).delete(id);
    return -1;
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    filter.addRule(new MockDeleteFilter<I>(id));
    return false;
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    Lock idLock = locks.getLockForId(id).writeLock();
    idLock.lock();
    return Collections.singleton(idLock);
  }
}
