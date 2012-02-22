/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface LogManager {
  
  Future<Void> write(LogRecord record);
  
  interface LogRecord {
    //private final byte[] data;
    
    long getLowestLsn();
  }  
}
