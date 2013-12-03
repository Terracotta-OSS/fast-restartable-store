/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.BufferSource;
import com.terracottatech.frs.io.Chunk;
import java.nio.ByteBuffer;
import java.util.Collections;

/**
 *
 * @author mscott
 */
public class CopyingPacker extends LogRegionPacker {
    
    private final BufferSource pool;
    private static final int FUTURE_SPACER = 64;
    
    public CopyingPacker(Signature sig, BufferSource copyInto) {
        super(sig);
        pool = copyInto;
    }   

    @Override
    protected Chunk writeRecords(Iterable<LogRecord> records) {     
      Chunk base = super.writeRecords(records);
      ByteBuffer grp = pool.getBuffer((int)base.length() + FUTURE_SPACER * 2);
      grp.position(FUTURE_SPACER);
      for ( ByteBuffer b : base.getBuffers() ) { 
        grp.put(b);
      }
      grp.flip();
      grp.position(FUTURE_SPACER);
      return new BufferListWrapper(Collections.singletonList(grp), pool);
    }
}
