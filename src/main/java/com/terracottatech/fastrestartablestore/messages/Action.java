/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore.messages;

/**
 *
 * @author cdennis
 */
public interface Action<K> {

  public boolean hasKey();

  public K getKey();
  
}
