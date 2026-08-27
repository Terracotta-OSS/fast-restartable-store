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
  
  Future<Void> append(LogRecord record);
  
  Future<Void> appendAndSync(LogRecord record);

  Snapshot snapshot() throws ExecutionException, InterruptedException;

  Future<Snapshot> snapshotAsync();
  
  IOStatistics getIOStatistics();
}
