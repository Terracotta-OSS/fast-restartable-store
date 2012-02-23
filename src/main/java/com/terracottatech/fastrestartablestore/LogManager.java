/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.fastrestartablestore;

import com.terracottatech.fastrestartablestore.IOManager.Chunk;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface LogManager {
  
  Future<Void> append(LogRecord record);

  Iterator<LogRecord> reader();
  
  interface LogRecord {
    //private final byte[] data;
    long getLsn();
    long getPreviousLsn();
    long getLowestLsn();
  }
  
  interface LogRegion extends Chunk {
    //collection of records and metadata (version, crc, etc.)
    
  }
}
