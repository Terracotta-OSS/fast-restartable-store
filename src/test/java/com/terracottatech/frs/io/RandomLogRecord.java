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
package com.terracottatech.frs.io;

import com.terracottatech.frs.log.LogRecord;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class RandomLogRecord implements LogRecord  {
    
    ByteBuffer[] list;
    int keyr = (int)(Math.random() * (2 * 1024));
    int valuer = (int)(Math.random() * (200 * 1024));
    final boolean sync = (Math.random() * (10)) < 1;
    long lsn;
    static FileInputStream randomizer;
    static {
        try {
            randomizer = new FileInputStream("/dev/random");
        } catch ( IOException ioe ) {
            
        }
    }
    
    public RandomLogRecord(long lsn) throws IOException {
        this();
        this.lsn = lsn;
    }

    public RandomLogRecord() throws IOException {
        this.lsn = lsn;
        double mutation = Math.random() * 2;
        if ( mutation < 1) {
            list = new ByteBuffer[1];
            keyr = keyr + (4-keyr%4);
            list[0] = ByteBuffer.allocate(keyr);
            while(list[0].hasRemaining()) {
                list[0].put("base".getBytes());
            }
            list[0].flip();
        } else {
            list = new ByteBuffer[2];
            keyr = keyr + (4-keyr%4);
            valuer = valuer + (4-valuer%4);
            list[0] = ByteBuffer.allocate(keyr);
            while(list[0].hasRemaining()) {
                list[0].put("base".getBytes());
            }            
            list[1] = ByteBuffer.allocate(valuer);
            while(list[1].hasRemaining()) {
                list[1].put("camp".getBytes());
            }
            list[0].flip();
            list[1].flip();
        }
        
    }

    @Override
    public ByteBuffer[] getPayload() {
        return list;
    }

    @Override
    public long getLsn() {
        return lsn;
    }

    @Override
    public void updateLsn(long lsn) {
        this.lsn = lsn;
    }

  @Override
  public void close() throws IOException {
    list = null;
  }
}
