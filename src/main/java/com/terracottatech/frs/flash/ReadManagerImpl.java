/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
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
  private final Cache cached = new Cache();
  
  public ReadManagerImpl(IOManager io) {
    this.ioManager = io;
  }
  
  private synchronized Chunk check(long lsn) {
      Chunk c = cached.get(lsn);
      if ( c != null ) {
        long pos = c.position();
        try {
          return c.getChunk(c.remaining());
        } finally { 
          c.clear();
          c.skip(pos);
        }
      }
      return null;
  }
  
  private synchronized void cache(long lsn, Chunk records) throws IOException {
      if ( records instanceof Loadable ) {
        ((Loadable)records).load();
      }
      cached.put(lsn, records);
  }

  @Override
  public LogRecord get(long marker) throws IOException {
    Chunk c = ioManager.scan(marker);
    try {
// maybe try and cache this
        LogRecord send = LogRegionPacker.extract(Signature.NONE, c, marker);
        
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
