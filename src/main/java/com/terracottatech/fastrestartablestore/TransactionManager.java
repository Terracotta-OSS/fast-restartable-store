/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import com.terracottatech.fastrestartablestore.messages.Action;

/**
 *
 * @author cdennis
 */
public interface TransactionManager {
  
  TransactionHandle begin();
  
  void commit(TransactionHandle handle);
  
  void happened(TransactionHandle handle, Action action);
}
