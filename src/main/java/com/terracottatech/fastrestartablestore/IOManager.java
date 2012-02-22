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
public interface IOManager {
  
  Future<Void> write(LogRegion region);

  interface LogRegion {
    //collection of records and metadata (version, crc, etc.)
    
    long getLowestLsn();
  }
}
