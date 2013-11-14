/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs.transaction;

import com.terracottatech.frs.action.Action;

/**
 *
 * @author mscott
 */
public class ExposedTransactionalAction extends TransactionalAction {

  public ExposedTransactionalAction(TransactionHandle handle, byte mode, Action action) {
    super(handle, mode, action);
  }

  public ExposedTransactionalAction(TransactionHandle handle, boolean begin, boolean commit, Action action, TransactionLSNCallback callback) {
    super(handle, begin, commit, action, callback);
  }

  @Override
  public Action getAction() {
    return super.getAction(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public TransactionHandle getHandle() {
    return super.getHandle(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isBegin() {
    return super.isBegin(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean isCommit() {
    return super.isCommit(); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void dispose() {
    super.dispose(); //To change body of generated methods, choose Tools | Templates.
  }
  
  
}
