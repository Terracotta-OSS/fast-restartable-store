/*
 * Copyright (c) 2013-2023 Software AG, Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA, and/or its subsidiaries and/or its affiliates and/or their licensors.
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
package com.terracottatech.frs.flash;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.io.Loadable;
import com.terracottatech.frs.log.FormatException;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mscott
 */
public class ReadManagerImpl implements ReadManager {
      
  private static final Logger LOGGER = LoggerFactory.getLogger(ReadManager.class);

  private final IOManager ioManager;
  private final String forceLogRegionFormat;
  private final Cache cached = new Cache();
  private int hit;
  private int miss;

  public ReadManagerImpl(IOManager io, String forceLogRegionFormat) {
    this.ioManager = io;
    this.forceLogRegionFormat = forceLogRegionFormat;
  }
// UNUSED  
  private synchronized Chunk check(long lsn) {
      Chunk c = cached.get(lsn);
      if ( c != null ) {
        hit++;
        long pos = c.position();
        try {
          return c.getChunk(c.remaining());
        } finally { 
          c.clear();
          c.skip(pos);
        }
      }
      miss++;
      return null;
  }
// UNUSED  
  private synchronized Chunk cache(long lsn, Chunk records) throws IOException {
      if ( records == null ) {
        return null;
      }
      if ( records instanceof Loadable ) {
        ((Loadable)records).load();
      }
      cached.put(lsn, records);
      long pos = records.position();
      try {
        return records.getChunk(records.remaining());
      } finally { 
        records.clear();
        records.skip(pos);
      }
    }

  @Override
  public LogRecord get(long marker) throws IOException {
    Chunk c = ioManager.scan(marker);
    try {
// maybe try and cache this
        LogRecord send = LogRegionPacker.extract(Signature.NONE, forceLogRegionFormat, c, marker);
        
        if ( send == null ) {
          throw new RuntimeException("not found");
        }
        
        return send;
    } catch ( FormatException form ) {
        throw new IOException(form);
    } finally {
        if ( c instanceof Closeable ) {
          ((Closeable)c).close();
        }
    }
  }
  
  static class Cache extends LinkedHashMap<Long, Chunk> {
        boolean over = false;

        public Cache() {
          super(256, 1.25f, true);
        }

        @Override
        public Chunk remove(Object key) {
            over = false;
            return super.remove(key); 
        }

        @Override
        protected boolean removeEldestEntry(Entry<Long, Chunk> eldest) {
            if ( over || this.size() > 512 ) {
                over = true;
                if (eldest.getValue() instanceof Closeable ) {
                  try {
                    ((Closeable)eldest.getValue()).close();
                  } catch ( IOException ioe ) {
                    LOGGER.warn("error closing chunk", ioe);
                  }
                }
            }
            return over;
        }
      
  }
  
}
