/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.messages;

import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author cdennis
 */
public interface Action {

  public long getLsn();
  
  public void record(long lsn);
  
  public void replay(long lsn);

  /*
   * compaction actions invalidate themselves here - they switch their record 
   * method to no-op and they make their binary representations empty
   */
  Collection<Lock> lock(TransactionLockProvider lockProvider);
}
