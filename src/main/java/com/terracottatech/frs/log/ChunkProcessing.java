/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * @author mscott
 */
public class ChunkProcessing implements Callable<List<LogRecord>> {
    
    private final Chunk           base;
    private FormatException     format;

    public ChunkProcessing(Chunk base) {
        this.base = base;
    }

    @Override
    public List<LogRecord> call() throws Exception {
        List<LogRecord> records = LogRegionPacker.unpack(Signature.ADLER32, base);
        Collections.reverse(records);
        return records;
    }
}
