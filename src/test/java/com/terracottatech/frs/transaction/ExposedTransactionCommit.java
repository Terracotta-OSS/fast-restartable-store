/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.transaction;

/**
 *
 * @author mscott
 */
public class ExposedTransactionCommit extends TransactionCommitAction {

  public ExposedTransactionCommit(TransactionHandle handle, boolean emptyTransaction) {
    super(handle, emptyTransaction);
  }
  
}
