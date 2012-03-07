/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.CompleteKey;
import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.TransactionLockProvider;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 *
 * @author cdennis
 */
public class MockCompactionAction<I, K, V> extends MockCompleteKeyAction<I, K> {

  private boolean valid = true;
  private Action compacted = null;
  
  public MockCompactionAction(CompleteKey<I, K> key) {
    super(key);
  }
  
  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    if (valid) {
      V value = ((ObjectManager<I, K, V> )objManager).replaceLsn(getId(), getKey(), lsn);
      compacted = new MockPutAction<I, K, V>(getId(), getKey(), value);
    }
    // XXX: This works because the the compacted action is just a duplicate of the put action we're compacting.
    // However this seems rather wrong because we'll be doing a put twice. Theoretically this shouldn't happen 
    // too often as we probably going to be deleted the chunks that have been compacted anyways.
    return -1;
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    if (valid) {
      return compacted.replay(filter, objManager, lsn);
    } else {
      return false;
    }
  }

  @Override
  public Collection<Lock> lock(TransactionLockProvider locks) {
    Lock lock = getLock(locks).writeLock();
    if (lock.tryLock()) {
      return Collections.singleton(lock);
    } else {
      valid = false;
      return Collections.emptyList();
    }
  }
}
