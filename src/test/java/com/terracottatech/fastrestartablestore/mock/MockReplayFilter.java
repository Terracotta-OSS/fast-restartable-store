/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Filter;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
class MockReplayFilter implements Filter<Action> {

  private final ObjectManager<?, ?, ?> objManager;

  public MockReplayFilter(ObjectManager<?, ?, ?> objManager) {
    this.objManager = objManager;
  }
  
  @Override
  public boolean filter(Action element, long lsn) {
    element.replay(lsn);
    return true;
  }
  
}
