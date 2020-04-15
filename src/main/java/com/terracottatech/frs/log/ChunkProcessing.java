/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.Loadable;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.Callable;

/**
 *
 * @author mscott
 */
public class ChunkProcessing implements Callable<List<LogRecord>> {
    
    private final Chunk           base;
    private final String forceLogRegionFormat;

    public ChunkProcessing(Chunk base, String forceLogRegionFormat) {
        this.base = base;
        this.forceLogRegionFormat = forceLogRegionFormat;
    }

    @Override
    public List<LogRecord> call() throws Exception {
      if ( base instanceof Loadable ) {
        ((Loadable)base).load();
      }
      try {
        List<LogRecord> records = LogRegionPacker.unpackInReverse(Signature.ADLER32, forceLogRegionFormat, base);
        return records;
      } finally {
        if ( base instanceof Closeable ) {
          ((Closeable)base).close();
        }
      }
    }
}
