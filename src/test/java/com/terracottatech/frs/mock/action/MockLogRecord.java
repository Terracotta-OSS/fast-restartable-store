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
package com.terracottatech.frs.mock.action;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.log.LogRecord;
import java.io.IOException;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 *
 * @author cdennis
 */
public class MockLogRecord implements LogRecord, Serializable {

  private final Action action;
  
  private long lsn = -1;
  
  MockLogRecord(Action action) {
    this.action = action;
  }

  public long getLsn() {
    return lsn;
  }

  public String toString() {
    String actionOut = action.toString();
    actionOut = "\t" + actionOut.replace("\n", "\n\t");
    
    return "LogRecord[lsn=" + getLsn() + " {\n"
            + actionOut + "\n"
            + "}";
  }

  public void updateLsn(long lsn) {
    this.lsn = lsn;
    action.record(lsn);
  }

  public Action getAction() {
    return action;
  }

    @Override
    public ByteBuffer[] getPayload() {
        
        return new ByteBuffer[0];
    }
  

  @Override
  public void close() throws IOException {

  }
}
