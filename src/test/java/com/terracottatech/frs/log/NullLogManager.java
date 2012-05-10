/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author tim
 */
public class NullLogManager implements LogManager {
  @Override
  public void updateLowestLsn(long lsn) {
  }

  @Override
  public long currentLsn() {
    return 0;
  }

  @Override
  public void startup() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public Future<Void> append(LogRecord record) {
    return null;
  }

  @Override
  public Future<Void> appendAndSync(LogRecord record) {
    return null;
  }

  @Override
  public Iterator<LogRecord> reader() {
    return Collections.<LogRecord>emptyList().iterator();
  }

    @Override
    public Future<Void> recover() {
        return null;
    }
  
  
}
