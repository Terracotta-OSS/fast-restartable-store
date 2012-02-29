/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Compactor;
import com.terracottatech.fastrestartablestore.RecordManager;
import com.terracottatech.fastrestartablestore.messages.Action;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 * 
 * @author cdennis
 */
class MockCompactor implements Compactor {

   private final RecordManager rcdManager;
   private final ObjectManager manager;

   public MockCompactor(RecordManager rcdManager, ObjectManager objectManager) {
      this.rcdManager = rcdManager;
      this.manager = objectManager;
   }

   public void compact(Action trigger) {
      if (true) {
         // XXX: We need a way to determine whether or not we're in a compaction. For now, this is just broken.
         return;
      }
      Action action = manager.checkoutEarliest(Long.MAX_VALUE);
      if (action != null) {
         try {
            rcdManager.asyncHappened(action);
         } finally {
            manager.checkin(action);
         }
      }
   }

}
