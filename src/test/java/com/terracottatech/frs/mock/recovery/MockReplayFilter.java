/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.mock.recovery;

import com.terracottatech.frs.recovery.Filter;
import com.terracottatech.frs.action.Action;

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
