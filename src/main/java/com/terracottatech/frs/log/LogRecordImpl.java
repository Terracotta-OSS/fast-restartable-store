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
package com.terracottatech.frs.log;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  Default implementation of a LogRecord.
 * 
 * This is the basic unit of persistence in the Log Stream
 * 
 * @author mscott
 */
public class LogRecordImpl implements LogRecord {
    
    private long lsn;
    private ByteBuffer[] payload;
    
    private final LSNEventListener listener;

    public LogRecordImpl(ByteBuffer[] buffers, LSNEventListener listener) {
        this.payload = buffers;
        this.listener = listener;
    }
    
    @Override
    public long getLsn() {
        return lsn;
    }

    @Override
    public ByteBuffer[] getPayload() {
        ByteBuffer[] bufs = new ByteBuffer[payload.length];
        for (int x=0;x<payload.length;x++) {
            bufs[x] = payload[x].asReadOnlyBuffer();
        }
        return bufs;
    }

    @Override
    public void updateLsn(long lsn) {
        this.lsn = lsn;
        if ( listener != null ) listener.record(lsn);
    }

    @Override
    public String toString() {
        return "LogRecordImpl{lsn=" + lsn + '}';
    }

  @Override
  public void close() throws IOException {
    payload = null;
  }
}
