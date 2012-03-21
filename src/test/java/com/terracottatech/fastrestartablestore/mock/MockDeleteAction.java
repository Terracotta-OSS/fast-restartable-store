package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

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
  public long getPreviousLsn() {
    return -1;
  }

  @Override
  public void record(long lsn) {
    objManager.delete(id);
  }

  @Override
  public void replay(long lsn) {
    throw new AssertionError();
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    Lock idLock = locks.getLockForId(id).writeLock();
    idLock.lock();
    return Collections.singleton(idLock);
  }
  
  public I getId() {
    return id;
  }
}
