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

  private final RecordManager<String, String> rcdManager;
  private final ObjectManager<String, String> manager;
  
  public MockCompactor(RecordManager<String, String> rcdManager, ObjectManager<String, String> objectManager) {
    this.rcdManager = rcdManager;
    this.manager = objectManager;
  }

  public void compact() {
    if (shouldCompact()) {
      try {
        Entry<String, String> e = manager.checkoutEarliest();
        if (e != null) {
          try {
            if (!isPartOfOpenTransaction(e)) {
              rcdManager.asyncHappened(new MockPutAction(e.getKey(), e.getValue()));
            }
          } finally {
            manager.checkin(e);
          }
        }
      } finally {
        isCompacting.remove();
      }
    }
  }
  
  private final ThreadLocal<Boolean> isCompacting = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  
  private boolean shouldCompact() {
    if (isCompacting.get()) {
      return false;
    } else {
      isCompacting.set(Boolean.TRUE);
      return true;
    }
  }

  private boolean isPartOfOpenTransaction(Entry<String, String> e) {
    //XXX This is wrong... who should figure this out and how?
    return true;
  }
}
