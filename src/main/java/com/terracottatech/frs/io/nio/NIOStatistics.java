/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.IOStatistics;
import java.io.File;

/**
 *
 * @author mscott
 */
public class NIOStatistics implements IOStatistics {
    long available;
    long totalUsed;
    long totalWrites;
    long totalReads;
    long dead;
    
    NIOStatistics(File directory, long used, long dead, long written, long reads) {
        available = directory.getUsableSpace();
        totalUsed = used;
        totalWrites = written;
        totalReads = reads;
        this.dead = dead;
    }

    @Override
    public long getExpiredSize() {
        return dead;
    }

    @Override
    public long getLiveSize() {
        return totalUsed - dead;
    }

    @Override
    public long getTotalAvailable() {
        return available;
    }

    @Override
    public long getTotalUsed() {
        return totalUsed;
    }

    @Override
    public long getTotalWritten() {
        return totalWrites;
    }
     @Override
    public long getTotalRead() {
        return totalReads;
    }

    @Override
    public String toString() {
        return "NIOStatistics{" + "available=" + available + ", totalUsed=" + totalUsed + ", totalWrites=" + totalWrites + ", totalReads=" + totalReads + ", dead=" + dead + '}';
    }
     
     
}
