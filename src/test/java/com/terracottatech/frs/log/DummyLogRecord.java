/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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

import com.terracottatech.frs.log.LogRecord;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class DummyLogRecord implements LogRecord {

    ByteBuffer[] list;
    int keyr = 1024;
    int valuer = 10 * 1024;
    long lsn;

    public DummyLogRecord(int ksize, int vsize) throws IOException {
        keyr = ksize;
        valuer = vsize;
        build();
    }

    public DummyLogRecord() throws IOException {
        keyr = 100;
        valuer = 1024;
        build();
    }

    private void build() {
        list = new ByteBuffer[2];
        keyr = keyr + (4 - keyr % 4);
        valuer = valuer + (4 - valuer % 4);
        list[0] = ByteBuffer.allocate(keyr);
//        while (list[0].hasRemaining()) {
//            list[0].put("base".getBytes());
//        }
        list[1] = ByteBuffer.allocate(valuer);
//        while (list[1].hasRemaining()) {
//            list[1].put("camp".getBytes());
//        }
//        list[0].flip();
//        list[1].flip();
    }

    @Override
    public ByteBuffer[] getPayload() {
        return list;
    }

    public void setLowestLsn(long lsn) {
    }

    @Override
    public long getLsn() {
        return lsn;
    }

    @Override
    public void updateLsn(long lsn) {
        this.lsn = lsn;
    }
    
    public long size() {
        return keyr + valuer;
    }
    

  @Override
  public void close() throws IOException {
    list = null;
  }
}
