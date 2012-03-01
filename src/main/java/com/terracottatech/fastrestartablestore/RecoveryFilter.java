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
public interface RecoveryFilter {

  public boolean replay(Action action, long lsn);
  
}
