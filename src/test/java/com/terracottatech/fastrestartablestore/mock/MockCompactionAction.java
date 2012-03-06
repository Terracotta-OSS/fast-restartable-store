/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.CompleteKey;
import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author cdennis
 */
public class MockCompactionAction<I, K> implements Action {

  public MockCompactionAction(CompleteKey<I, K> key) {
    
  }
  
  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Collection<Lock> lock(List<ReadWriteLock> locks) {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  
}
