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
