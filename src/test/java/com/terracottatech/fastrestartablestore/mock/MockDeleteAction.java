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

public class MockDeleteAction<I> implements MockAction {

  private final I id;
  private transient ObjectManager<I, ?, ?> objManager;
  
  public MockDeleteAction(ObjectManager<I, ?, ?> objManager, I id) {
    this.id = id;
    this.objManager = objManager;
  }

  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    this.objManager = (ObjectManager<I, ?, ?>) objManager;
  }

  @Override
  public long getLsn() {
    return -1;
  }

  @Override
  public void record(long lsn) {
    objManager.delete(id);
  }

  @Override
  public boolean replay(ReplayFilter filter, long lsn) {
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
