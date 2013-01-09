/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.Snapshot;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 * @author cdennis
 */
public interface LogManager {

  long currentLsn();
  
  void updateLowestLsn(long lsn);

  long lowestLsn();

  Iterator<LogRecord> startup();

  void shutdown();
  
//  Future<Void> recover();
  
  Future<Void> append(LogRecord record);
  
  Future<Void> appendAndSync(LogRecord record);

  Snapshot snapshot() throws ExecutionException, InterruptedException;
}
