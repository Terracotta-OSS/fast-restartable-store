/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Filter;
import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
class MockReplayFilter implements Filter<Action> {

  @Override
  public boolean filter(Action element, long lsn) {
    element.replay(lsn);
    return true;
  }
  
}
