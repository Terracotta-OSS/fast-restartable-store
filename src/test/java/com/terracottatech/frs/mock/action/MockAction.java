/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.mock.action;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.object.ObjectManager;

public interface MockAction extends Action {
  // Just an ugly hack interface to get around the deserialization aspect of the mock.
  public void setObjectManager(ObjectManager<?, ?, ?> objManager);
}
