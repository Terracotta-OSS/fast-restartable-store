/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.terracottatech.frs.log;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface LogManager {
  
  Future<Void> append(LogRecord record);

  Iterator<LogRecord> reader();
}
