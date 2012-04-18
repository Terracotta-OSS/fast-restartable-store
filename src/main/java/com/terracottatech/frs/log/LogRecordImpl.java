/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import java.nio.ByteBuffer;

/**
 *  Default implementation of a LogRecord.
 * 
 * This is the basic unit of persistence in the Log Stream
 * 
 * @author mscott
 */
public class LogRecordImpl implements LogRecord {
    
    private long lowestLsn;
    private long lsn;
    private final ByteBuffer[] payload;
    
    private final LSNEventListener listener;

    public LogRecordImpl(long lowestLsn, ByteBuffer[] buffers, LSNEventListener listener) {
        this.lowestLsn = lowestLsn;
        this.payload = buffers;
        this.listener = listener;
    }

    @Override
    public long getLowestLsn() {
        return lowestLsn;
    }

    @Override
    public void setLowestLsn(long lsn) {
        lowestLsn = lsn;
    }
    
    @Override
    public long getLsn() {
        return lsn;
    }

    @Override
    public ByteBuffer[] getPayload() {
        ByteBuffer[] bufs = new ByteBuffer[payload.length];
        for (int x=0;x<payload.length;x++) {
            bufs[x] = payload[x].duplicate();
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
        return "LogRecordImpl{" + "lowestLsn=" + lowestLsn + ", lsn=" + lsn + '}';
    }
}
