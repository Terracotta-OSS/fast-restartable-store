/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.transaction;

/**
 * @author tim
 */
public interface TransactionLSNCallback {
  void setLsn(TransactionHandle handle, long lsn);
}
