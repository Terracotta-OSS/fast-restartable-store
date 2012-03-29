/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

/**
 *
 * @author cdennis
 */
public interface LogRecord {

  //private final byte[] data;
  //private final byte[] data;
  long getLsn();

  long getPreviousLsn();

  long getLowestLsn();
  
  void updateLsn(long lsn);
}
