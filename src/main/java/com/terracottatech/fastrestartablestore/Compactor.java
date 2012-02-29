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
public interface Compactor {
  
  void compact(Action action);
}
