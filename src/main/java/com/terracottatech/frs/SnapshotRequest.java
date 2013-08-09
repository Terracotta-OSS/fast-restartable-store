/*
 * All content copyright (c) 2013 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs;

import com.terracottatech.frs.Snapshot;

/**
 *
 * @author mscott
 */
public interface SnapshotRequest {
    void setSnapshot(Snapshot snap);
}
