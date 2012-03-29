/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import java.util.concurrent.locks.ReadWriteLock;

public interface TransactionLockProvider {

  public ReadWriteLock getLockForKey(Object id, Object key);
  
  public ReadWriteLock getLockForId(Object id);
}
