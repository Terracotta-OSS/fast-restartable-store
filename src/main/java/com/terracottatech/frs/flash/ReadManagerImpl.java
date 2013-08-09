/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package com.terracottatech.frs.flash;

import com.terracottatech.frs.io.Chunk;
import com.terracottatech.frs.io.IOManager;
import com.terracottatech.frs.log.FormatException;
import com.terracottatech.frs.log.LogRecord;
import com.terracottatech.frs.log.LogRegionPacker;
import com.terracottatech.frs.log.Signature;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author mscott
 */
public class ReadManagerImpl implements ReadManager {
  
  private final IOManager ioManager;

  public ReadManagerImpl(IOManager io) {
    this.ioManager = io;
  }

  @Override
  public synchronized LogRecord get(long marker) {
    try {
      Chunk c = ioManager.scan(marker);
      List<LogRecord> records = LogRegionPacker.unpack(Signature.ADLER32, c);
      for ( LogRecord r : records ) {
        if ( r.getLsn() == marker ) {
          return r;
        }
      }
    } catch ( IOException ioe ) {
      
    } catch ( FormatException form ) {
      
    }
    return null;
  }
  
}
