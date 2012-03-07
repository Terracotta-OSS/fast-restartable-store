package com.terracottatech.fastrestartablestore;

import java.util.concurrent.locks.ReadWriteLock;

public interface TransactionLockProvider {

  public ReadWriteLock getLockForKey(Object id, Object key);
  
  public ReadWriteLock getLockForId(Object id);
}
