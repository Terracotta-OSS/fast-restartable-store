/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.messages;

import com.terracottatech.fastrestartablestore.spi.ObjectManager;
import java.util.Set;

/**
 *
 * @author cdennis
 */
public interface Action<K, V> {

  public boolean hasKey();

  public K getKey();

  public boolean replay(ObjectManager<K, V> objManager, Set<Long> validTxnIds, long lsn);
}
