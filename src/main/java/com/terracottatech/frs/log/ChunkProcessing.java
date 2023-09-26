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
