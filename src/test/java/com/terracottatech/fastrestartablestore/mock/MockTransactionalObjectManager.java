package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.RecoveryFilter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

public class MockTransactionalObjectManager implements ObjectManager {

  private final ObjectManager objManager;
  private Map<Long, Long> openTxnIds = new LinkedHashMap<Long, Long>();

  public MockTransactionalObjectManager(ObjectManager objManager) {
    this.objManager = objManager;
  }

  @Override
  public long getLowestLsn() {
    return objManager.getLowestLsn();
  }

  @Override
  public long record(Action action, long lsn) {
    if (action instanceof MockTransactionalAction) {
      return objManager.record(((MockTransactionalAction) action).getEmbedded(), lsn);
    } else if (action instanceof MockTransactionBeginAction) {
      openTxnIds.put(((MockTransactionBeginAction) action).getId(), lsn);
      return -1;
    } else if (action instanceof MockTransactionCommitAction) {
      openTxnIds.remove(((MockTransactionCommitAction) action).getId());
      return -1;
    } else {
      return objManager.record(action, lsn);
    }
  }

  @Override
  public RecoveryFilter createRecoveryFilter() {
    return new RecoveryFilterImpl(objManager.createRecoveryFilter());
  }

  @Override
  public Action checkoutEarliest(long ceilingLsn) {
    return objManager.checkoutEarliest(Math.min(ceilingLsn, getCeilingLsn()));
  }

  private long getCeilingLsn() {
    Iterator<Long> i = openTxnIds.values().iterator();
    if (i.hasNext()) {
      return i.next();
    } else {
      return Long.MAX_VALUE;
    }
  }

  @Override
  public void checkin(Action action) {
    objManager.checkin(action);
  }

  @Override
  public int size() {
    return objManager.size();
  }

  static class RecoveryFilterImpl implements RecoveryFilter {

    private final Set<Long> validTxnIds = new HashSet<Long>();
    private final RecoveryFilter next;
    
    private RecoveryFilterImpl(RecoveryFilter next) {
      this.next = next;
    }

    @Override
    public boolean replay(Action action, long lsn) {
      if (action instanceof MockTransactionCommitAction) {
        validTxnIds.add(((MockTransactionCommitAction) action).getId());
        return false;
      } else if (action instanceof MockTransactionBeginAction) {
        validTxnIds.remove(((MockTransactionBeginAction) action).getId());
        return false;
      } else if (action instanceof MockTransactionalAction) {
        if (validTxnIds.contains(((MockTransactionalAction) action).getId())) {
          next.replay(((MockTransactionalAction) action).getEmbedded(), lsn);
          return true;
        } else {
          return false;
        }
      } else {
        return next.replay(action, lsn);
      }
    }
    
  }
}
