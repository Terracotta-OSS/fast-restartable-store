/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
