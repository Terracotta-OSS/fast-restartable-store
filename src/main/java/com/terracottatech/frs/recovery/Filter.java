/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.recovery;

/**
 *
 * @author cdennis
 */
public interface Filter<T> {
  
  /**
   * Returns {@code true} if the record forms a valid part of the log.
   */
  boolean filter(T element, long lsn);
}
