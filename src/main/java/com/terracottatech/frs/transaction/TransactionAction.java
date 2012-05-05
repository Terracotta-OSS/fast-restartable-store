package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

/**
 * @author tim
 */
interface TransactionAction extends Action {
  TransactionHandle getHandle();
  boolean isCommit();
  boolean isBegin();
}
