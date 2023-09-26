/*
 * Copyright (c) 2012-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.io.nio;

import com.terracottatech.frs.io.IOStatistics;
import java.io.File;

/**
 *
 * @author mscott
 */
public class NIOStatistics implements IOStatistics {
    private final long available;
    private final long totalUsed;
    private final long totalWrites;
    private final long totalReads;
    private final long dead;
    
    NIOStatistics(File directory, long used, long dead, long written, long reads) {
        this.available = directory.getUsableSpace();
        this.totalUsed = used;
        this.totalWrites = written;
        this.totalReads = reads;
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
        return " available: " + available + " totalUsed:" + totalUsed + " totalWrites:" + totalWrites + " totalReads:" + totalReads + " dead:" + dead;
    }
     
     
}
