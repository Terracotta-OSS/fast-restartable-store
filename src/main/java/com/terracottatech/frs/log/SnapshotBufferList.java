/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.Snapshot;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mscott
 */
public class SnapshotBufferList extends BufferListWrapper implements SnapshotRequest {
    
    List<SnapshotRequest> holder = new LinkedList<SnapshotRequest>();

    public SnapshotBufferList(List<ByteBuffer> base, List<SnapshotRequest> holder) {
      super(base);
        this.holder = holder;
    }

    @Override
    public void setSnapshot(Snapshot snap) {
        for (SnapshotRequest req : holder ) {
            req.setSnapshot(snap);
        }
    }

}
