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
    final int keyr = (int)(Math.random() * (2 * 1024));
    final int valuer = (int)(Math.random() * (200 * 1024));
    final int headerr = (int)(34);
    final boolean sync = (Math.random() * (10)) < 1;
    long lsn;
    static FileInputStream randomizer;
    static {
        try {
            randomizer = new FileInputStream("/dev/random");
        } catch ( IOException ioe ) {
            
        }
    }

    public TestLogRecord() throws IOException {
        double mutation = Math.random() * 2;
        if ( mutation < 1) {
            list = new ByteBuffer[1];
            list[0] = ByteBuffer.allocate(headerr);
        } else {
            list = new ByteBuffer[3];
            list[0] = ByteBuffer.allocate(headerr);
            list[1] = ByteBuffer.allocate(keyr);
            list[2] = ByteBuffer.allocate(valuer);
        }
        randomizer.getChannel().read(list);
    }

    @Override
    public ByteBuffer[] getPayload() {
        return list;
    }

    @Override
    public long getLowestLsn() {
        return 5;
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
