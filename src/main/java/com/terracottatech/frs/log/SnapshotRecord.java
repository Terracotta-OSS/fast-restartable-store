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
import java.util.Iterator;

/**
 *
 * @author mscott
 */
public class SnapshotRecord implements Snapshot, LogRecord, SnapshotRequest {
    private long lsn;
    private volatile Snapshot delegate;

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public Iterator<File> iterator() {
        return delegate.iterator();
    }
    
    @Override
    public void setSnapshot(Snapshot snap) {
        this.delegate = snap;
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
