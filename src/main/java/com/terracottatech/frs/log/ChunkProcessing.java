/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * @author mscott
 */
public class ChunkProcessing implements Callable<List<LogRecord>> {
    
    private final Chunk           base;

    public ChunkProcessing(Chunk base) {
        this.base = base;
    }

    @Override
    public List<LogRecord> call() throws Exception {
        List<LogRecord> records = LogRegionPacker.unpackInReverse(Signature.ADLER32, base);
        return records;
    }
}
