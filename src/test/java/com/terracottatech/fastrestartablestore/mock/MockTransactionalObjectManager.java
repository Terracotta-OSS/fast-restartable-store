package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
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
}
