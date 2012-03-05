package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

public class MockDeleteAction<I> implements Action {

  private final I id;
  
  public MockDeleteAction(I id) {
    this.id = id;
  }

  @Override
  public long record(ObjectManager<?, ?, ?> objManager, long lsn) {
    ((ObjectManager<I, ?, ?>) objManager).recordDelete(id, lsn);
    return -1;
  }

  @Override
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn) {
    ((ObjectManager<I, ?, ?>) objManager).replayDelete(id, lsn);
    filter.addRule(new MockDeleteFilter<I>(id));
    return false;
  }
}
