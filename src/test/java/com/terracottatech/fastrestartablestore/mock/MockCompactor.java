/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Compactor;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Map.Entry;

/**
 *
 * @author cdennis
 */
class MockCompactor implements Compactor {

  private final RecordManager<String> rcdManager;
  private final ObjectManager<String, String> manager;
  
  public MockCompactor(RecordManager<String> rcdManager, ObjectManager<String, String> objectManager) {
    this.rcdManager = rcdManager;
    this.manager = objectManager;
  }

  public void compact() {
    if (shouldCompact()) {
      Entry<String, String> e = manager.checkoutEarliest();
      try {
        rcdManager.asyncHappened(new MockPutAction(e.getKey(), e.getValue()));
      } finally {
        manager.checkin(e);
      }
    }
  }
  
  private boolean shouldCompact() {
    return true;
  }
}
