/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
public class TestLogRecord implements LogRecord  {
    
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
    
    public TestLogRecord(long lsn) throws IOException {
        this();
        this.lsn = lsn;
    }

    public TestLogRecord() throws IOException {
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
    public long getLowestLsn() {
        return 5;
    }
    
    public void setLowestLsn(long lsn) {
        
    }

    @Override
    public long getLsn() {
        return lsn;
    }

    @Override
    public long getPreviousLsn() {
        return this.lsn - 1;
    }

    @Override
    public void updateLsn(long lsn) {
        this.lsn = lsn;
    }
    
}
