/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.log;

import com.terracottatech.frs.io.AbstractChunk;
import com.terracottatech.frs.io.BufferSource;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 * @author mscott
 */
public class BufferListWrapper extends AbstractChunk implements Closeable {
    
    private final ByteBuffer[]  converted;
    private final BufferSource  source;
    
    public BufferListWrapper(List<ByteBuffer> base) {
        converted = base.toArray(new ByteBuffer[base.size()]);
        this.source = null;
    }    
    
    public BufferListWrapper(List<ByteBuffer> base, BufferSource source) {
        converted = base.toArray(new ByteBuffer[base.size()]);
        this.source = source;
    }

    @Override
    public ByteBuffer[] getBuffers() {
        return converted;
    }

    @Override
    public void close() throws IOException {
      if ( source != null ) {
        for ( ByteBuffer bb : converted ) {
          source.returnBuffer(bb);
        }
      }
    }
}
