/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
}
