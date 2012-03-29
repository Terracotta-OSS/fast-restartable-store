/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.compaction;

import com.terracottatech.frs.mock.action.MockAction;
import com.terracottatech.frs.mock.MockCompleteKeyAction;
import com.terracottatech.frs.mock.MockPutAction;
import com.terracottatech.frs.object.CompleteKey;
import com.terracottatech.frs.transaction.TransactionLockProvider;
import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.object.ObjectManager;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author cdennis
 */
public class MockCompactionAction<I, K, V> extends MockCompleteKeyAction<I, K> implements MockAction {

  private boolean valid = true;
  private Action compacted = null;
  private transient final ObjectManager<I, K, V> objManager;
  
  public MockCompactionAction(ObjectManager<I, K, V> objManager, CompleteKey<I, K> key) {
    super(key);
    this.objManager = objManager;
  }
  
  @Override
  public void setObjectManager(ObjectManager<?, ?, ?> objManager) {
    if (compacted instanceof MockAction) {
      ((MockAction) compacted).setObjectManager(objManager);
    }
  }

  @Override
  public long getPreviousLsn() {
    // XXX: This works because the the compacted action is just a duplicate of the put action we're compacting.
    // However this seems rather wrong because we'll be doing a put twice. Theoretically this shouldn't happen 
    // too often as we're probably going to be deleting the chunks that have been compacted anyways.
    return -1;
  }

  @Override
  public void record(long lsn) {
    if (valid) {
      V value = objManager.replaceLsn(getId(), getKey(), lsn);
      compacted = new MockPutAction<I, K, V>(objManager, getId(), getKey(), value);
    }
  }

  @Override
  public void replay(long lsn) {
    if (valid) {
      compacted.replay(lsn);
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

  @Override
  public String toString() {
    return "CompactionAction : compacted=" + compacted; 
  }
}
