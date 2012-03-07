/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.messages;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author cdennis
 */
public interface Action {

  public long record(ObjectManager<?, ?, ?> objManager, long lsn);
  
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn);

  /*
   * compaction actions invalidate themselves here - they switch their record 
   * method to no-op and they make their binary representations empty
   */
  Collection<Lock> lock(TransactionLockProvider lockProvider);
}
