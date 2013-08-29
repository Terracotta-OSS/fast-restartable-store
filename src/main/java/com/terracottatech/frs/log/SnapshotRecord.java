/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.Snapshot;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

/**
 *
 * @author mscott
 */
public class SnapshotRecord implements Snapshot, LogRecord, SnapshotRequest {
    private long lsn;
    private volatile Snapshot delegate;

    @Override
    public synchronized void close() throws IOException {
        if ( delegate != null ) {
            delegate.close();
        }
    }

    @Override
    public synchronized Iterator<File> iterator() {
        try {
            while ( delegate == null ) {
                this.wait();
            }
        } catch ( InterruptedException ie ) {
            Thread.currentThread().interrupt();
            return Collections.<File>emptyList().iterator();
        }
        return delegate.iterator();
    }
    
    @Override
    public synchronized void setSnapshot(Snapshot snap) {
        this.delegate = snap;
        this.notifyAll();
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
    public ByteBuffer[] getPayload() {
        return new ByteBuffer[0];
    }
     
}
