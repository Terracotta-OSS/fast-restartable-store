/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
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
