/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs;

/**
 *
 * @author cdennis
 */
public interface RestartStore<I, K, V> {
  
  Transaction<I, K, V> beginTransaction();
}
