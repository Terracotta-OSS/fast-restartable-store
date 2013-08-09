/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.Snapshot;
import com.terracottatech.frs.io.AbstractChunk;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mscott
 */
public class SnapshotBufferList extends AbstractChunk implements SnapshotRequest {
    
    private final ByteBuffer[] base; 
    List<SnapshotRequest> holder = new LinkedList<SnapshotRequest>();

    public SnapshotBufferList(ByteBuffer[] base, List<SnapshotRequest> holder) {
        this.base = base;
        this.holder = holder;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return base;
    }

    @Override
    public void setSnapshot(Snapshot snap) {
        for (SnapshotRequest req : holder ) {
            req.setSnapshot(snap);
        }
    }

}
