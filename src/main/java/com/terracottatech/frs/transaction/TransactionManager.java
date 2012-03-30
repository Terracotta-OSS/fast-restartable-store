/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

import com.terracottatech.frs.TransactionException;
import com.terracottatech.frs.action.Action;

/**
 *
 * @author cdennis
 */
public interface TransactionManager {
  
  TransactionHandle begin();
  
  void commit(TransactionHandle handle) throws InterruptedException, TransactionException;
  
  void happened(TransactionHandle handle, Action action);
}
