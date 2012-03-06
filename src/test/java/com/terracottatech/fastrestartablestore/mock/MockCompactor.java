/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.mock;

import com.terracottatech.fastrestartablestore.Compactor;
import com.terracottatech.fastrestartablestore.CompleteKey;
import com.terracottatech.fastrestartablestore.TransactionHandle;
import com.terracottatech.fastrestartablestore.TransactionManager;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 * 
 * @author cdennis
 */
class MockCompactor<I, K, V> implements Compactor {

   private final TransactionManager txnManager;
   private final ObjectManager<I, K, V> manager;
   
   private final ThreadLocal<Boolean> compacting = new ThreadLocal<Boolean>() {
      @Override
      protected Boolean initialValue() {
         return false;
      }
   };

   public MockCompactor(TransactionManager txnManager, ObjectManager<I, K, V> objectManager) {
      this.txnManager = txnManager;
      this.manager = objectManager;
   }

   public void compact() {
      if (compacting.get()) {
         // Prevent recursive compaction
         return;
      }
      CompleteKey<I, K> key = manager.getCompactionKey();
      if (key != null) {
         compacting.set(true);
         try {
           TransactionHandle txnHandle = txnManager.create();
           txnManager.happened(txnHandle, new MockCompactionAction(key));
           txnManager.commit(txnHandle);
         } finally {
            compacting.set(false);
         }
      }
   }

}
