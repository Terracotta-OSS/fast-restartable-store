/*
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.io.IOStatistics;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author tim
 */
public class NullLogManager implements LogManager {
  @Override
  public void updateLowestLsn(long lsn) {
  }

  @Override
  public long lowestLsn() {
    return 0;
  }

  @Override
  public long currentLsn() {
    return 0;
  }

  @Override
  public Iterator<LogRecord> startup() {
      return reader();
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

  private Iterator<LogRecord> reader() {
    return Collections.<LogRecord>emptyList().iterator();
  }

  @Override
  public Snapshot snapshot() throws ExecutionException, InterruptedException {
    return null;
  }

  @Override
  public Future<Snapshot> snapshotAsync() {
    return null;
  }

  @Override
  public IOStatistics getIOStatistics() {
      return null;      
  }
  
  
}
