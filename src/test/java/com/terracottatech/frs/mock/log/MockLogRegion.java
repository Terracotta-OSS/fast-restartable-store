/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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
package com.terracottatech.frs.mock.log;

import com.terracottatech.frs.log.LogRegion;
import com.terracottatech.frs.log.LogRecord;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 *
 * @author cdennis
 */
class MockLogRegion implements LogRegion, Serializable {

  final LogRecord record;
  
  public MockLogRegion(LogRecord record) {
    this.record = record;
  }

    @Override
    public Iterator<LogRecord> iterator() {
        return Arrays.asList(record).iterator();
    }

  public String toString() {
    String recordOut = record.toString();
    recordOut = "\t" + recordOut.replace("\n", "\n\t");
    
    return "LogRegion[] {\n"
            + recordOut + "\n}";
  }
  
  private Object writeReplace() {
    return record;
  }
}
