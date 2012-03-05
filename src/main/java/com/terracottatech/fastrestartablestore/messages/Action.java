/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.messages;

import com.terracottatech.fastrestartablestore.ReplayFilter;
import com.terracottatech.fastrestartablestore.spi.ObjectManager;

/**
 *
 * @author cdennis
 */
public interface Action {

  public long record(ObjectManager<?, ?, ?> objManager, long lsn);
  
  public boolean replay(ReplayFilter filter, ObjectManager<?, ?, ?> objManager, long lsn);
}
