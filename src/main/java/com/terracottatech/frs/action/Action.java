/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.action;

import com.terracottatech.frs.transaction.TransactionLockProvider;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author cdennis
 */
public interface Action {

  public long getPreviousLsn();
  
  public void record(long lsn);
  
  public void replay(long lsn);

  /*
   * compaction action invalidate themselves here - they switch their record
   * method to no-op and they make their binary representations empty
   */
  Collection<Lock> lock(TransactionLockProvider lockProvider);
}
