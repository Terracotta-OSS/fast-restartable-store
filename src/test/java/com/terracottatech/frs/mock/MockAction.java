package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

public interface MockAction extends Action {
  // Just an ugly hack interface to get around the deserialization aspect of the mock.
  public void setObjectManager(ObjectManager<?, ?, ?> objManager);
}
