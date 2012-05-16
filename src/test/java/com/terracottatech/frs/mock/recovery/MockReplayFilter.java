/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (filtered) {
      return false;
    } else {
      element.replay(lsn);
      return true;
    }
  }
  
}
